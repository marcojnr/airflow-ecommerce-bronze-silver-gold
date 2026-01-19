from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, date
import pandas as pd
import io

data_hoje = date.today()
BUCKET = "olist-data-lake"

TABLES = {
    "orders": {
        "csv": "bronze/orders/orders.csv",
        "parquet": "silver/orders/orders.parquet",
        "date_cols": ["order_purchase_timestamp", "order_delivered_customer_date"]
    },
    "order_items": {
        "csv": "bronze/order_items/order_items.csv",
        "parquet": "silver/order_items/order_items.parquet"
    },
    "payments": {
        "csv": "bronze/payments/payments.csv",
        "parquet": "silver/payments/payments.parquet"
    },
    "reviews": {
        "csv": "bronze/reviews/reviews.csv",
        "parquet": "silver/reviews/reviews.parquet"
    },
    "customers": {
        "csv": "bronze/customers/customers.csv",
        "parquet": "silver/customers/customers.parquet"
    },
    "products": {
        "csv": "bronze/products/products.csv",
        "parquet": "silver/products/products.parquet"
    },
    "sellers": {
        "csv": "bronze/sellers/sellers.csv",
        "parquet": "silver/sellers/sellers.parquet"
    },
    "geolocation": {
        "csv": "bronze/geolocation/geolocation.csv",
        "parquet": "silver/geolocation/geolocation.parquet"
    }
}


def csv_to_parquet(table_name, config):
    s3 = S3Hook(aws_conn_id="minio_s3")

    # Download CSV
    obj = s3.get_key(config["csv"], bucket_name=BUCKET)
    df = pd.read_csv(obj.get()["Body"])

    # Limpeza básica
    df = df.drop_duplicates()

    # Conversão de datas
    for col in config.get("date_cols", []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Escreve parquet em memória
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    # Upload para Silver
    s3.load_file_obj(
        file_obj=buffer,
        key=config["parquet"],
        bucket_name=BUCKET,
        replace=True
    )


with DAG(
    dag_id="silver_olist_transform",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "olist", "parquet"],
) as dag:

    for table, cfg in TABLES.items():
        PythonOperator(
            task_id=f"silver_{table}",
            python_callable=csv_to_parquet,
            op_kwargs={"table_name": table, "config": cfg}
        )
