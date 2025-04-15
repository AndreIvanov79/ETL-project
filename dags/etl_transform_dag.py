from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

def run_data_transform():
    from src.transform.data_transformer import CommonDataTransformer
    transformer = CommonDataTransformer()
    try:
        result = transformer.transform_all()
        print("Transformation result:", result)
    finally:
        transformer.close()

with DAG(
    dag_id='etl_data_transformation',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl']
) as dag:
    transform_task = PythonOperator(
        task_id='data_transformation',
        python_callable=run_data_transform
    )

    trigger_loader = TriggerDagRunOperator(
        task_id='trigger_loader_dag',
        trigger_dag_id='etl_data_loader',
        wait_for_completion=True
    )

    transform_task >> trigger_loader
    