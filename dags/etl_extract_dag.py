from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

def run_data_extraction():
    from src.extract.data_extraction import DataExtraction
    extractor = DataExtraction()
    try:
        success = extractor.extract_all_data()
        if not success:
            raise Exception("Data extraction failed")
    finally:
        extractor.close()

with DAG(
    dag_id='etl_data_extraction',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=['etl']
) as dag:
    extract_task = PythonOperator(
        task_id='data_extraction',
        python_callable=run_data_extraction
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transformation_dag',
        trigger_dag_id='etl_data_transformation',
        wait_for_completion=True
    )

    extract_task >> trigger_transform
    