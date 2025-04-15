from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

def run_data_loader():
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from src.load import data_loader
    data_loader.prepare_reporting_tables()
    for table, fetch_method in data_loader.export_targets.items():
        print(f"Export table: {table}")
        try:
            rows = fetch_method()
            data_loader.save_table(table, rows)
        except Exception as e:
            print(f"Error during table {table} export: {e}")

with DAG(
    dag_id='etl_data_loader',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etl']
) as dag:
    data_loader_task = PythonOperator(
        task_id='data_loader_export',
        python_callable=run_data_loader
    )

    trigger_streamlit = TriggerDagRunOperator(
        task_id='trigger_streamlit_dag',
        trigger_dag_id='etl_streamlit_reporting',
        wait_for_completion=False
    )

    data_loader_task >> trigger_streamlit
    