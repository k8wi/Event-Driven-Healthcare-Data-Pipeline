from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateClusterOperator, DataprocDeleteClusterOperator

PROJECT_ID = "gen-lang-client-0215307817"
REGION = "us-central1"
CLUSTER_NAME = "medicare-pipeline-cluster-stage3"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 6, 1),
}
dag = DAG(
    'stage_3_scd_type2_merge',
    default_args=default_args,
    description='Stage 3: SCD Type 2 Dimensions & Fact Table Creation',
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
	"software_config": {
        "image_version": "2.0-debian10",
        "properties": {
            "spark:spark.jars.packages":
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0"
        }
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
        "main_python_file_uri": "gs://cms-partd-data-demo/scripts/stage_3_pyspark_scd2.py",
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