from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def run_streamlit_reporting():
    import subprocess, time, webbrowser
    process = subprocess.Popen(
        ["streamlit", "run", "src/load/streamlit_reporting.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    print("Streamlit dashboard is running on http://localhost:8501")
    try:
        webbrowser.open("http://localhost:8501")
    except:
        print("Failed to open browser automatically")
    time.sleep(10)

with DAG(
    dag_id='etl_streamlit_reporting',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['report']
) as dag:
    streamlit_reporting_task = PythonOperator(
        task_id='launch_streamlit_dashboard',
        python_callable=run_streamlit_reporting
    )
    