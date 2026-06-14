from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import json

# ==========================================
# 1. Configuration & DB Connection
# ==========================================
API_URL = "https://kolzsticks.github.io/Free-Ecommerce-Products-Api/main/products.json"
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
def init_db_table():
    """Drop old table and create a new expanded one (Schema Evolution)."""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("DROP TABLE IF EXISTS products CASCADE;")
    
    create_table_query = """
    CREATE TABLE products (
        id VARCHAR(50) PRIMARY KEY,
        name VARCHAR(255),
        category VARCHAR(100),
        sub_category VARCHAR(100),
        price_cents INT,
        rating_stars FLOAT,
        rating_count INT,
        keywords JSONB,
        description TEXT,
        image_url TEXT
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("[+] Expanded Products table created in PostgreSQL.")

def fetch_and_store_products():
    """Fetch all API data and insert into PostgreSQL."""
    # 1. Fetch Data
    response = requests.get(API_URL)
    response.raise_for_status()
    products = response.json()
    
    # 2. Store Data
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO products (id, name, category, sub_category, price_cents, rating_stars, rating_count, keywords, description, image_url)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    count = 0
    for p in products:
        rating_stars = p.get("rating", {}).get("stars", 0.0)
        rating_count = p.get("rating", {}).get("count", 0)
        
        keywords_json = json.dumps(p.get("keywords", []))
        
        cursor.execute(insert_query, (
            p.get("id"), 
            p.get("name"), 
            p.get("category"),
            p.get("subCategory"),
            p.get("priceCents", 0),
            rating_stars,
            rating_count,
            keywords_json,
            p.get("description"),
            p.get("image")
        ))
        count += 1
        
    conn.commit()
    cursor.close()
    conn.close()
    print(f"[+] Successfully stored {count} full products in database.")

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
    'daily_product_catalog_sync',
    default_args=default_args,
    description='Fetch FULL products from API and store in Postgres',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ingestion', 'api', 'postgres'],
) as dag:

    task_create_table = PythonOperator(
        task_id='create_products_table',
        python_callable=init_db_table,
    )

    task_fetch_store = PythonOperator(
        task_id='fetch_and_store_products',
        python_callable=fetch_and_store_products,
    )

    task_create_table >> task_fetch_store