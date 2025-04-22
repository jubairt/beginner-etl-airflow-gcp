from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime




default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 18)
}

with DAG(
    'csv_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    run_etl = BashOperator(
        task_id='run_etl_script',
        bash_command='python /home/airflow/gcs/dags/scripts/extract_transform_load.py',
    )
    

    
    
