from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from modules import extraction, transformation, load


with DAG(
    dag_id="holtzy_etl",
    default_args={
        "owner": "fabriciositto",
        "depends_on_past": False,
        "start_date": datetime(2023, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "catch"
        "retry_delay": timedelta(minutes=5),
    },
    description="ETL Holtzy repos",
    schedule_interval="@daily",
    catchup=False
) as dag:
  
    args = [f"{datetime.now().strftime('%Y-%m-%d %H')}", os.getcwd()]

    task_extraction = PythonOperator(
        task_id="extract_data",
        python_callable=extraction,
        op_args=args,
        
    )
    
    task_transformation = PythonOperator(
        task_id="transform_data",
        python_callable=transformation,
        op_args=args,
        
    )
    

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=load,
        op_args=args,
    )

    task_extraction >>  task_transformation >> task_load
    


