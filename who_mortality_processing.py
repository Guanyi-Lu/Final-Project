from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import Variable
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import os
import io
import zipfile

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

# manually added the variables in airflow UI
RAW_BUCKET = Variable.get("gcs_bucket_raw")  
PROCESSED_BUCKET = Variable.get("gcs_bucket_parquet")

BQ_DATASET = Variable.get("bq_dataset_name")  # Project_BigQuery_Dataset
WHO_DATA_URL = "https://www.who.int/data/data-collection-tools/who-mortality-database"

with DAG(
    'who_mortality_processing',
    default_args=default_args,
    schedule_interval='0 0 1 * *',  # Run monthly on the 1st
    catchup=False,
    max_active_runs=1,
) as dag:

    # ========== TASK 1: Download ZIP to Raw Bucket ==========
    def download_to_gcs(**kwargs):
        """Download WHO mortality ZIP files to raw bucket"""
        gcs_hook = GCSHook()
        response = requests.get(WHO_DATA_URL)
        soup = BeautifulSoup(response.text, "html.parser")
        
        for link in soup.find_all('a', href=True):
            if ".zip" in link['href']:
                file_url = urljoin(WHO_DATA_URL, link['href'])
                filename = os.path.basename(urlparse(file_url).path)
                
                # Stream download directly to GCS
                with requests.get(file_url, stream=True) as response:
                    response.raise_for_status()
                    gcs_hook.upload(
                        bucket_name=RAW_BUCKET,
                        object_name=f"raw/{filename}",
                        data=response.content,
                        mime_type='application/zip'
                    )
                print(f"Downloaded {filename} to gs://{RAW_BUCKET}/raw/{filename}")

    download_task = PythonOperator(
        task_id='download_raw_data',
        python_callable=download_to_gcs,
    )

    # ========== TASK 2: Process to Cleaned Parquet ==========
    spark_clean_job = DataprocSubmitJobOperator(
        task_id="clean_and_prepare_data",
        job={
            "reference": {
                "project_id": "{{ var.value.GCP_GUANYI_PROJECT }}",
                "job_id": f"who-clean-{{{{ ts_nodash }}}}" #make the job id uniquely different each time
            },
            "placement": {
                "cluster_name": "cluster-14bf"
            },
            "pyspark_job": {
                "main_python_file_uri": f"gs://{PROCESSED_BUCKET}/scripts/spark_data_prep.py",
                
                #"python_file_uris": [f"gs://{PROCESSED_BUCKET}/scripts/requirements.txt"],
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.sources.bucketing.enabled": "true",
                    #"spark.submit.pyFiles": f"gs://{PROCESSED_BUCKET}/scripts/requirements.txt"
                    #"dataproc.pip.packages": "openpyxl==3.0.10 pandas==1.5.3"
                }
            }
        },
        region="{{ var.value.REGION }}",
        project_id="{{ var.value.GCP_GUANYI_PROJECT }}",
    )

    # ========== TASK 3: Aggregate to BigQuery ==========
    spark_aggregate_job = DataprocSubmitJobOperator(
        task_id="aggregate_to_bigquery",
        job={
            "reference": {
                "project_id": "{{ var.value.GCP_GUANYI_PROJECT }}",
                "job_id": f"who-clean-agg{{{{ ts_nodash }}}}" #make the job id uniquely different each time
                 
            },
            "placement": {
                "cluster_name": "cluster-14bf"  #my cluster name
            },
            "pyspark_job": {
                "main_python_file_uri": f"gs://{PROCESSED_BUCKET}/scripts/spark_aggregation.py",
                "args": [
                    
                "--input", f"gs://{PROCESSED_BUCKET}/australia_data/",
                "--output", f"{BQ_DATASET}.Australia_Mortality_Aggregated_Table"

                   ],
                #"jar_file_uris": [
                  #  "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar"
                #],
                "properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.shuffle.partitions": "200"
                }
            }
        },
        region="{{ var.value.REGION }}",
        project_id="{{ var.value.GCP_GUANYI_PROJECT }}",
    )

    # ===== WORKFLOW =====
    download_task >> spark_clean_job >> spark_aggregate_job