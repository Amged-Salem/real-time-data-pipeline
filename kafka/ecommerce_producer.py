import json
import time
import random
import uuid
import psycopg2
from datetime import datetime
from kafka import KafkaProducer

# === OpenTelemetry Imports ===
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# ==========================================
# 1. Configuration & Tracing Setup
# ==========================================
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "ecommerce_orders"
DB_CONFIG = {
    "host": "localhost",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": "5432"
}

# --- Initialize OpenTelemetry Tracer ---
resource = Resource(attributes={"service.name": "python-order-producer"})
trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer(__name__)

# Send traces to Jaeger (listening on port 4318 for HTTP)
otlp_exporter = OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces")
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# ==========================================
# 2. Database Connection (Fetch Dimension Data)
# ==========================================
def get_products_from_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, category, sub_category, price_cents FROM products;")
        rows = cursor.fetchall()
        
        products = []
        for row in rows:
            products.append({
                "id": row[0],
                "name": row[1],
                "category": row[2],
                "sub_category": row[3],
                "priceCents": row[4]
            })
            
        cursor.close()
        conn.close()
        print(f"[+] Loaded {len(products)} products from Database.")
        return products
    except Exception as e:
        print(f"[-] Database Error: {e}")
        return []

# ==========================================
# 3. Kafka Producer Setup
# ==========================================
def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

# ==========================================
# 4. Data Generation Logic
# ==========================================
def generate_order(products):
    product = random.choice(products)
    quantity = random.randint(1, 4)
    total_cents = product["priceCents"] * quantity
    statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"]
    
    order_event = {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.randint(1000, 9999),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "sub_category": product["sub_category"],
        "quantity": quantity,
        "total_price_cents": total_cents,
        "order_status": random.choice(statuses),
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    return order_event

# ==========================================
# 5. Main Streaming Loop with Tracing
# ==========================================
def main():
    products = get_products_from_db()
    if not products:
        print("[-] No products found.")
        return

    producer = create_producer()
    print(f"[*] Starting Generator on topic: '{TOPIC_NAME}'...")

    try:
        while True:
            order = generate_order(products)
            
            # --- Start a Trace Span for this order ---
            with tracer.start_as_current_span("generate_and_send_order") as span:
                # Add useful tags to the trace
                span.set_attribute("order.id", order["order_id"])
                span.set_attribute("order.status", order["order_status"])
                span.set_attribute("product.category", order["category"])

                # Inject trace context into Kafka headers
                headers = {}
                TraceContextTextMapPropagator().inject(headers)
                
                # Convert headers dict to list of tuples (Kafka format)
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]

                # Send to Kafka WITH the trace headers
                producer.send(TOPIC_NAME, value=order, headers=kafka_headers)

            # Terminal Output
            status_color = "🟢" if order["order_status"] == "COMPLETED" else ("🟡" if order["order_status"] == "PENDING" else "🔴")
            print(f"{status_color} [{order['timestamp']}] Order {order['order_id'][:8]}... sent with Trace ID.")
            
            time.sleep(random.uniform(0.5, 2.5))
            
    except KeyboardInterrupt:
        print("\n[*] Stopping Generator safely...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()