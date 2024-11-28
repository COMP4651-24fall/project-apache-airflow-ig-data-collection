from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import papermill as pm
import os
from dotenv import load_dotenv

load_dotenv()

base_script_path = os.getenv('BASE_SCRIPT_PATH')
print(f"base script path: {base_script_path}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 0,
}

dag = DAG(
    'data_scrapping',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "account_name": "default_account",  # Set a default value if needed
    },
)

# Define the PythonOperator with provide_context set to True
data_scrapping = BashOperator(
    task_id='data_scrapping',
    bash_command=(
        f'python3 {base_script_path}/data_scrapping.py '
        '{{ params.account_name }}'  # Pass account_name as a parameter
    ),
    dag=dag,
)

data_scrapping
