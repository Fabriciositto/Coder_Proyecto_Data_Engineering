from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
from modules import extraction, transformation, load
import logging


with DAG(
    dag_id="holtzy_etl",
    description="ETL Holtzy repos",
    schedule_interval="@daily",
    catchup=False
) as dag:
  
    
    task_extraction = PythonOperator(
        task_id="extract_data",
        python_callable=extraction
        
    )
    
    task_transformation = PythonOperator(
        task_id="transform_data",
        python_callable=transformation
        
    )
    

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=load
    )

    task_extraction >>  task_transformation >> task_load
    


