from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT_ID = "gen-lang-client-0215307817"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 6, 1),
}
dag = DAG(
    'main_pipeline_orchestrator',
    default_args=default_args,
    description='Master Orchestrator: Chains all 4 pipeline stages sequentially',
    schedule_interval=None,
    catchup=False,
)

trigger_stage_1 = TriggerDagRunOperator(
    task_id='trigger_stage_1_raw_ingestion',
    trigger_dag_id='stage_1_raw_ingestion',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_stage_2 = TriggerDagRunOperator(
    task_id='trigger_stage_2_data_cleansing',
    trigger_dag_id='stage_2_data_cleansing',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_stage_3 = TriggerDagRunOperator(
    task_id='trigger_stage_3_scd_type2_merge',
    trigger_dag_id='stage_3_scd_type2_merge',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_stage_4 = TriggerDagRunOperator(
    task_id='trigger_stage_4_dq_checks',
    trigger_dag_id='stage_4_dq_checks',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

trigger_stage_1 >> trigger_stage_2 >> trigger_stage_3 >> trigger_stage_4