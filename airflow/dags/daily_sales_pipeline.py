from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

# ==========================================
# 1. Configuration & DB Connection
# ==========================================
DB_CONFIG = {
    "host": "postgres", 
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": "5432"
}

# ==========================================
# 2. Python Functions for Tasks
# ==========================================
def check_data_quality():
    """DQ Check: Ensure no negative prices exist in raw_orders."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Query to check for any negative prices
    cursor.execute("SELECT COUNT(*) FROM raw_orders WHERE total_price_cents < 0;")
    negative_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    if negative_count > 0:
        # Stop the pipeline immediately if there is a data issue
        raise ValueError(f"[-] Data Quality Check FAILED: Found {negative_count} orders with negative prices!")
    
    print("[+] Data Quality Check PASSED. 0 negative prices found.")

def aggregate_daily_sales():
    """Aggregate sales by date and category, then upsert to daily_sales_summary."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Data aggregation query (UPSERT)
    # Aggregates data; if the date and category already exist, it updates them instead of duplicating
    agg_query = """
    INSERT INTO daily_sales_summary (sales_date, category, total_orders, total_revenue_cents)
    SELECT 
        DATE(order_timestamp) as sales_date,
        category,
        COUNT(order_id) as total_orders,
        SUM(total_price_cents) as total_revenue_cents
    FROM raw_orders
    GROUP BY DATE(order_timestamp), category
    ON CONFLICT (sales_date, category) 
    DO UPDATE SET 
        total_orders = EXCLUDED.total_orders,
        total_revenue_cents = EXCLUDED.total_revenue_cents;
    """
    
    cursor.execute(agg_query)
    conn.commit()
    
    cursor.close()
    conn.close()
    print("[+] Daily sales aggregated and updated successfully in daily_sales_summary.")

# ==========================================
# 3. DAG Definition
# ==========================================
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'daily_sales_aggregation_pipeline',
    default_args=default_args,
    description='DQ Check and Aggregate Daily Sales',
    schedule_interval='@daily', # Runs automatically once a day
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['processing', 'aggregation', 'dq'],
) as dag:

    # Define tasks
    task_dq_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
    )

    task_aggregate = PythonOperator(
        task_id='aggregate_sales',
        python_callable=aggregate_daily_sales,
    )

    # Define task dependencies (Execution order)
    task_dq_check >> task_aggregate