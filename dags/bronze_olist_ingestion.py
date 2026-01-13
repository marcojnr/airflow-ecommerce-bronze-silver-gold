from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, date
import os

data_hoje = date.today()
BUCKET_NAME = "olist-data-lake"

DATASETS = {
    "orders": "orders.csv",
    "order_items": "order_items.csv",
    "payments": "payments.csv",
    "reviews": "reviews.csv",
    "customers": "customers.csv",
    "products": "products.csv",
    "sellers": "sellers.csv",
    "geolocation": "geolocation.csv",
}

LOCAL_BASE_PATH = "/opt/airflow/data/raw"


def upload_to_bronze():
    s3 = S3Hook(aws_conn_id="minio_s3")

    for domain, filename in DATASETS.items():
        local_path = os.path.join(LOCAL_BASE_PATH, filename)
        s3_key = f"bronze/{domain}/{filename}"

        s3.load_file(
            filename=local_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )


with DAG(
    dag_id="bronze_olist_ingestion",
    start_date=data_hoje,
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "olist", "s3"],
) as dag:

    ingest_bronze = PythonOperator(
        task_id="upload_csv_to_bronze",
        python_callable=upload_to_bronze
    )

    ingest_bronze
