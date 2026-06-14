# Real-Time E-Commerce Data Pipeline with Agentic AI

A production-grade, fully containerized data pipeline combining **real-time stream processing** with **scheduled batch jobs**, built on the Lambda Architecture pattern.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        REAL-TIME E-COMMERCE DATA PIPELINE                       │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌─────────────────┐     ┌──────────────────────────────────────────────────┐
  │   EXTERNAL API   │     │                  BATCH LAYER                     │
  │  products.json   │────►│              Apache Airflow                      │
  └─────────────────┘     │   ┌─────────────────┐  ┌───────────────────┐    │
                           │   │ fetch_products   │  │ daily_sales_aggr  │    │
                           │   │     DAG          │  │      DAG          │    │
                           │   │  @daily          │  │  @daily           │    │
                           │   └────────┬─────────┘  └────────┬──────────┘   │
                           └────────────┼───────────────────────┼─────────────┘
                                        │                       │
                                        ▼                       ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              STORAGE LAYER                                       │
│                           PostgreSQL (port 5432)                                 │
│                                                                                  │
│   ┌─────────────┐        ┌──────────────┐        ┌──────────────────────┐       │
│   │  products   │        │  raw_orders  │        │  daily_sales_summary │       │
│   │  (catalog)  │        │  (streaming) │        │  (aggregated batch)  │       │
│   └─────────────┘        └──────────────┘        └──────────────────────┘       │
└───────────────────────────────▲──────────────────────────────────────────────────┘
                                │ JDBC Write
                                │
┌───────────────────────────────┼──────────────────────────────────────────────────┐
│                    STREAM PROCESSING LAYER                                       │
│                                                                                  │
│    ┌────────────────────────────────────────────────────┐                        │
│    │              Apache Spark (port 7077)              │                        │
│    │                                                    │                        │
│    │   spark-master ──── spark-worker                  │                        │
│    │        │                                          │                        │
│    │   streaming_job.py                                │                        │
│    │   • Read from Kafka                               │                        │
│    │   • Parse JSON schema                             │                        │
│    │   • Transform timestamps                          │                        │
│    │   • Write to PostgreSQL (foreachBatch)            │                        │
│    │   • Checkpointing enabled                         │                        │
│    └────────────────────────▲───────────────────────────┘                        │
└─────────────────────────────┼────────────────────────────────────────────────────┘
                              │ Kafka Consumer
                              │
┌─────────────────────────────┼────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                                │
│                                                                                  │
│   ┌──────────────────┐      │       ┌─────────────────────────────────────┐      │
│   │ ecommerce_       │      │       │         Apache Kafka                 │      │
│   │ producer.py      │──────┼──────►│                                     │      │
│   │                  │  publish     │  Topic: ecommerce_orders            │      │
│   │ • Reads products │      │       │  Broker: kafka:29092                │      │
│   │ • Generates fake │      │       │  Zookeeper: zookeeper:2181          │      │
│   │   orders         │      │       └─────────────────────────────────────┘      │
│   │ • ~1 msg/sec     │                                                           │
│   └──────────────────┘                                                           │
└──────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────┐
│                         OBSERVABILITY LAYER                                      │
│                                                                                  │
│  ┌─────────────────┐    ┌──────────────┐    ┌──────────────┐                    │
│  │   Prometheus     │    │   Grafana    │    │ Alertmanager │                    │
│  │   (port 9090)    │───►│  (port 3000) │    │  (port 9093) │                    │
│  │                  │    │              │    │              │                    │
│  │  Scrapes:        │    │  Dashboards: │    │  Routes to:  │                    │
│  │  • kafka-exp     │    │  • Spark     │    │  • Slack     │                    │
│  │  • postgres-exp  │    │  • Kafka     │    │  • Email     │                    │
│  │  • spark-master  │    │  • Pipeline  │    │              │                    │
│  │  • spark-worker  │    │              │    │  Alert Rules:│                    │
│  │  • spark-driver  │    └──────────────┘    │  • SparkDown │                    │
│  └─────────────────┘                         │  • KafkaDown │                    │
│                                              │  • PGDown    │                    │
│  ┌─────────────────┐    ┌──────────────┐    └──────────────┘                    │
│  │    Promtail      │    │     Loki     │                                        │
│  │   (log agent)    │───►│  (port 3100) │                                        │
│  │                  │    │              │                                        │
│  │  Collects logs   │    │  Log storage │                                        │
│  │  from all        │    │  & querying  │                                        │
│  │  containers      │    │              │                                        │
│  └─────────────────┘    └──────────────┘                                        │
│                                                                                  │
│  ┌─────────────────┐                                                             │
│  │     Jaeger       │                                                             │
│  │   (port 16686)   │                                                             │
│  │  Distributed     │                                                             │
│  │  Tracing         │                                                             │
│  └─────────────────┘                                                             │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Stream Ingestion | Apache Kafka | 7.4.0 |
| Stream Processing | Apache Spark | 3.4.1 |
| Batch Orchestration | Apache Airflow | 2.7.1 |
| Storage | PostgreSQL | 13 |
| Metrics | Prometheus | 2.45.0 |
| Visualization | Grafana | 10.0.3 |
| Log Aggregation | Loki + Promtail | 2.9.0 |
| Alerting | Alertmanager | 0.26.0 |
| Tracing | Jaeger | latest |
| Container Runtime | Docker Compose | - |

---

## Project Structure

```
real_time_data_pipeline_with_agentic_aI/
├── airflow/
│   ├── dags/
│   │   ├── daily_sales_pipeline.py      # Batch aggregation + DQ DAG
│   │   └── fetch_products_dag.py         # Product catalog sync DAG
│   ├── logs/
│   └── plugins/
├── alertmanager/
│   └── alertmanager.yml                  # Alert routing (Slack/Email)
├── kafka/
│   └── ecommerce_producer.py             # Real-time order generator
├── loki/
│   └── loki-config.yaml                  # Loki log storage config
├── prometheus/
│   ├── prometheus.yml                    # Scrape targets
│   └── alert_rules.yml                   # Alert rules (8 rules)
├── promtail/
│   └── config.yml                        # Log collection config
├── spark/
│   ├── streaming_job.py                  # PySpark Structured Streaming
│   └── metrics.properties                # Prometheus metrics config
├── Spark Grafana Dashboard/
│   └── spark-dashboard.json              # Pre-built Grafana dashboard
├── docker-compose.yml                    # Full infrastructure (14 services)
├── requirements.txt                      # Python dependencies
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker Desktop (WSL2 mode, 8GB RAM minimum)
- Python 3.8+

### 1. Clone & Start Infrastructure

```bash
git clone https://github.com/YOUR_USERNAME/real_time_data_pipeline_with_agentic_aI.git
cd real_time_data_pipeline_with_agentic_aI
docker-compose up -d
```

### 2. Copy Spark Metrics Config
```bash
docker exec spark_master mkdir -p /opt/spark/conf
docker exec spark_worker mkdir -p /opt/spark/conf
docker exec spark_master cp /opt/workspace/spark/metrics.properties /opt/spark/conf/metrics.properties
docker exec spark_worker cp /opt/workspace/spark/metrics.properties /opt/spark/conf/metrics.properties
docker-compose restart spark-master spark-worker
```

### 3. Trigger Airflow DAGs
1. Open http://localhost:8082 (admin/admin)
2. Enable & trigger `daily_product_catalog_sync`
3. Wait for it to complete (green)

### 4. Start Kafka Producer
```bash
pip install -r requirements.txt
python kafka/ecommerce_producer.py
```

### 5. Start Spark Streaming Job
```bash
docker exec -it spark_master \
  /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
  --master spark://spark-master:7077 \
  /opt/workspace/spark/streaming_job.py
```

### 6. Import Grafana Dashboard
1. Open http://localhost:3000 (admin/admin)
2. Dashboards → New → Import
3. Upload `Spark Grafana Dashboard/spark-dashboard.json`

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:8083 | - |
| Spark Worker UI | http://localhost:8081 | - |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| Alertmanager | http://localhost:9093 | - |
| Jaeger | http://localhost:16686 | - |
| Loki | http://localhost:3100 | - |
| PostgreSQL | localhost:5432 | airflow / airflow |
| Kafka | localhost:9092 | - |

---

## Alert Rules

| Alert | Condition | Severity |
|-------|-----------|----------|
| SparkWorkerDown | alive workers < 1 for 1m | Critical |
| SparkMemoryHigh | memory > 1800MB for 2m | Warning |
| SparkNoRunningApps | running apps = 0 for 5m | Warning |
| KafkaDown | exporter unreachable for 1m | Critical |
| KafkaConsumerLagHigh | lag > 1000 msgs for 2m | Warning |
| PostgresDown | exporter unreachable for 1m | Critical |
| PostgresTooManyConnections | connections > 80 for 2m | Warning |
| PrometheusTargetDown | any target down for 2m | Warning |

---

## Data Flow

```
External API → Airflow → PostgreSQL (products)
                                    ↓
                          Kafka Producer reads products
                                    ↓
                          Kafka Topic: ecommerce_orders
                                    ↓
                          Spark Structured Streaming
                                    ↓
                          PostgreSQL (raw_orders)
                                    ↓
                          Airflow → daily_sales_summary
```
