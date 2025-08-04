import os
import sys
import boto3
import pandas as pd
import mysql.connector
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("transactions_pipeline")

# Path Setup 
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

# Load .env
load_dotenv()

# Import transformation function
from scripts.transform import transform_data

# Global File Paths 
TMP_DIR = os.path.join(project_root, 'data', 'tmp')
os.makedirs(TMP_DIR, exist_ok=True)

RAW_CSV = os.path.join(TMP_DIR, 'transactions.csv')
TRANSFORMED_CSV = os.path.join(TMP_DIR, 'transformed_transactions.csv')

# DAG Configuration 
default_args = {
    'owner': 'victor',
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='transactions_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    description='3-step pipeline: download, transform, load banking data'
)

# Task 1: Download 
def download_csv_from_s3():
    try:
        logger.info("Starting download from S3")
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            region_name=os.getenv("AWS_REGION")
        )

        bucket = os.getenv("S3_BUCKET")
        key = os.getenv("S3_KEY")

        s3.download_file(bucket, key, RAW_CSV)
        logger.info(f"Downloaded S3 file to {RAW_CSV}")

    except Exception as e:
        logger.error(f"Failed to download from S3: {e}")
        raise

# Task 2: Transform 
def transform_task():
    try:
        logger.info("🛠️ Starting data transformation")
        transform_data(input_path=RAW_CSV, output_path=TRANSFORMED_CSV)
        logger.info(f"Transformed data saved to {TRANSFORMED_CSV}")
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise

# Task 3: Load 
def load_to_mysql():
    try:
        logger.info("Starting load to MySQL")
        transformed_df = pd.read_csv(TRANSFORMED_CSV)
        logger.info(f"Read {len(transformed_df)} rows from transformed CSV")

        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DB")
        )

        cursor = conn.cursor()
        for i, row in transformed_df.iterrows():
            try:
                cursor.execute("""
                    INSERT INTO transaction_aggregates (bank_id, date, total_transaction_volume)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    total_transaction_volume = VALUES(total_transaction_volume)
                """, (row['bank_id'], row['date'], row['total_transaction_volume']))
            except Exception as row_err:
                logger.warning(f"Skipped row {i} due to error: {row_err}")

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Successfully loaded data into MySQL")

    except Exception as e:
        logger.error(f"MySQL load failed: {e}")
        raise

# DAG Task Definitions 
download_task = PythonOperator(
    task_id='download_csv_from_s3',
    python_callable=download_csv_from_s3,
    dag=dag
)

transform_csv_task = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_task,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_to_mysql',
    python_callable=load_to_mysql,
    dag=dag
)

# Task Order 
download_task >> transform_csv_task >> load_task

# End of DAG 
logger.info("DAG transactions_pipeline is successful.")