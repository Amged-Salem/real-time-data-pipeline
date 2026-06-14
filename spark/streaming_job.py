from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# === OpenTelemetry Imports ===
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# ==========================================
# 1. Initialize OpenTelemetry Tracer
# ==========================================
resource = Resource(attributes={"service.name": "spark-order-consumer"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# Send traces to Jaeger (Using the internal Docker network name 'jaeger')
otlp_exporter = OTLPSpanExporter(endpoint="http://jaeger:4318/v1/traces")
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# ==========================================
# 2. Initialize Spark Session & Enable Prometheus
# ==========================================
spark = SparkSession.builder \
    .appName("KafkaToPostgresStreaming") \
    .config("spark.ui.prometheus.enabled", "true") \
    .config("spark.executor.processTreeMetrics.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# 3. Define Data Schema (Matching Kafka Producer)
# ==========================================
schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("sub_category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_price_cents", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# ==========================================
# 4. Read Stream from Kafka (Now with headers)
# ==========================================
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "ecommerce_orders") \
    .option("startingOffsets", "latest") \
    .option("includeHeaders", "true")\
    .load()

# ==========================================
# 5. Transformations (Extracting Value AND Trace ID)
# ==========================================
# We use Spark SQL 'expr' to extract 'traceparent' from the Kafka headers array
parsed_df = kafka_df.selectExpr(
    "CAST(value AS STRING)",
    "CAST(filter(headers, x -> x.key == 'traceparent')[0].value AS STRING) as traceparent"
).select(
    from_json(col("value"), schema).alias("data"),
    col("traceparent")
).select("data.*", "traceparent")

final_df = parsed_df.withColumn("order_timestamp", to_timestamp(col("timestamp"))) \
    .drop("timestamp")

# ==========================================
# 6. Write Stream to PostgreSQL with Checkpointing & Tracing
# ==========================================
def write_to_postgres(df, epoch_id):
    if df.isEmpty():
        return
        
    # Extract the traceparent from the first row to link this batch to the order's journey
    first_row = df.select("traceparent").first()
    carrier = {}
    if first_row and first_row["traceparent"]:
        carrier["traceparent"] = first_row["traceparent"]

    # Extract context using OpenTelemetry
    ctx = TraceContextTextMapPropagator().extract(carrier=carrier)

    # Start a span linked to the Producer's trace
    with tracer.start_as_current_span("spark_write_to_postgres", context=ctx) as span:
        record_count = df.count()
        span.set_attribute("batch.epoch_id", epoch_id)
        span.set_attribute("batch.record_count", record_count)
        span.set_attribute("db.system", "postgresql")
        span.set_attribute("db.table", "raw_orders")
        
        # Drop the traceparent column before writing to DB (it's not in the DB schema)
        db_df = df.drop("traceparent")
        
        db_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/airflow") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "raw_orders") \
            .option("user", "airflow") \
            .option("password", "airflow") \
            .mode("append") \
            .save()

print("[*] Starting Spark Streaming Job with Tracing, Checkpointing & JMX Sink...")

query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/spark_checkpoints/orders_streaming_v7") \
    .outputMode("append") \
    .start()

query.awaitTermination()