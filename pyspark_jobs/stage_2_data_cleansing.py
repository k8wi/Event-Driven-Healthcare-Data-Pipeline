from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

PROJECT_ID = "gen-lang-client-0215307817"
REGION = "us-central1"
CLUSTER_NAME = "medicare-pipeline-cluster-stage2"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 6, 1),
}
dag = DAG(
    'stage_2_data_cleansing',
    default_args=default_args,
    description='Stage 2: Data Cleansing & Transformation via PySpark',
    schedule_interval=None,
    catchup=False,
)

cluster_config = {
    
        "gce_cluster_config": {
            "zone_uri": f"{REGION}-a",
        },
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",
        },
        "worker_config": {
            "num_instances": 2,
            "machine_type_uri": "e2-standard-2",
        },
        "lifecycle_config": {
            "idle_delete_ttl": {"seconds":900},
        },
        "software_config": {
            "image_version": "2.0-debian10",
        },
    }

spark_job = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://cms-partd-data-demo/scripts/stage_2_pyspark_cleanse.py",
    },
}
create_cluster = DataprocCreateClusterOperator(
    task_id="create_dataproc_cluster",
    project_id=PROJECT_ID,
    cluster_config=cluster_config,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    dag=dag,
)

submit_spark_job = DataprocSubmitJobOperator(
    task_id="submit_spark_job",
    job=spark_job,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_dataproc_cluster",
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule="all_done",
    dag=dag,
)

create_cluster >> submit_spark_job >> delete_cluster