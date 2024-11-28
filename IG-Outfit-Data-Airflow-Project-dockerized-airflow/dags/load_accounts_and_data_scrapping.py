from airflow import DAG
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import pandas as pd
import os
from airflow.operators.bash_operator import BashOperator

# Load base data path from .env

# Load environment variables and proxies
def load_proxies_and_accounts():
    load_dotenv()
    
    # Load proxies from .env
    http_proxies = [proxy.strip() for proxy in os.getenv('HTTP_PROXY_LIST', '').split(',')]
    https_proxies = [proxy.strip() for proxy in os.getenv('HTTPS_PROXY_LIST', '').split(',')]


    # Load accounts
    base_data_path = os.getenv('BASE_DATA_PATH')
    df = pd.read_csv(f"{base_data_path}/target_accounts.csv")
    account_names = df['account_name'].unique().tolist()
    
    return http_proxies, https_proxies, account_names

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 1),
    'retries': 1,
}

with DAG(
        dag_id='data_scrapping_and_load_media_combined',
        schedule_interval=None,
        default_args={
            "owner": "airflow",
            "retries": 15,
            "retry_delay": timedelta(seconds=10),
            "start_date": datetime(2021, 1, 1),
        },
        max_active_tasks=8,  # Limits to 1 concurrent tasks within this DAG
        max_active_runs=1,  # Limits to 1 concurrent DAG run
        catchup=False) as dag:
    
    # Dynamically create tasks to trigger the data_scrapping DAG for each account_name
    http_proxies, https_proxies, account_names = load_proxies_and_accounts()
    base_script_path = os.getenv('BASE_SCRIPT_PATH')
    for idx, account_name in enumerate(account_names):
        
        # Select proxies based on 
        http_proxy = http_proxies[idx % len(http_proxies)]
        https_proxy = https_proxies[idx % len(https_proxies)]

        data_scrapping = BashOperator(
            task_id=f'data_scrapping_{account_name}',
            bash_command=(
                f'python3 {base_script_path}/data_scrapping_and_load_media_combined.py '
                f'--account_name {account_name} --http_proxy {http_proxy} --https_proxy {https_proxy}'  # Pass account_name as a parameter
            ),
            dag=dag,
        )

        data_scrapping

