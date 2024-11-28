from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import papermill as pm
import os
from dotenv import load_dotenv

#load base script path from .env
load_dotenv()
base_script_path = os.getenv('BASE_SCRIPT_PATH')
print(f"base script path: {base_script_path}")


# Define functions to run jyputer notebooks
def run_read_accounts():
    pm.execute_notebook(
        input_path=f'{base_script_path}/read_accounts.ipynb',  # Path to your notebook
        output_path=f'{base_script_path}/read_accounts.ipynb',  # Where to save the executed notebook
    )
# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 0,
    
}

dag = DAG(
    'read_accounts',
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

# Define the PythonOperator to call the function
read_accounts = PythonOperator(
    task_id='read_accounts',
    python_callable=run_read_accounts,
    dag=dag,
)

read_accounts
