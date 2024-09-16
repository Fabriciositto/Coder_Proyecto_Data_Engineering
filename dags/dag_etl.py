from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
from modules import extraction, transformation, load, email_manager

default_args={
        "owner": "fabriciositto",
        "depends_on_past": False,
        "start_date": datetime(2023, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=1),
        'on_failure_callback': email_manager.email_manager,
        'on_success_callback': email_manager.email_manager,
    }


with DAG(
    dag_id="holtzy_etl",
    default_args=default_args,
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

    #  # Verifica la existencia de la data extraida
    # data_extracted_file_sensor = FileSensor(
    #     task_id='data_extracted_file_sensor',
    #     fs_conn_id='fs_default',
    #     filepath=f'{os.getcwd()}/raw_data/data.json',
    #     poke_interval=30,  
    #     timeout=600,  
    #     mode='poke',  
    #     dag=dag
    # )

    
    
    task_transformation = PythonOperator(
        task_id="transform_data",
        python_callable=transformation,
        op_args=args,
        
    )
    
    # # Verifica la existencia de la data transformada
    # data_transformed_file_sensor = FileSensor(
    #     task_id='data_transformed_file_sensor',
    #     fs_conn_id='fs_default',
    #     filepath=f'{os.getcwd()}/raw_data/data.csv',
    #     poke_interval=30,  
    #     timeout=600,  
    #     mode='poke',  
    #     dag=dag
    # )

    task_load = PythonOperator(
        task_id="load_data",
        python_callable=load,
        op_args=args,
    )
    # task_extraction >> data_extracted_file_sensor >> task_transformation >> data_transformed_file_sensor >> task_load

    task_extraction  >> task_transformation  >> task_load
    


