from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
        dag_id='main',
        schedule_interval=None,
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(seconds=240),
            "start_date": datetime(2021, 1, 1),
        },
        tags=['main'],
        max_active_tasks=4,  # Limits to 3 concurrent tasks within this DAG
        max_active_runs=1,  # Limits to 1 concurrent DAG run
        catchup=False) as dag:
    
    # Create a task to trigger the read_accounts DAG (without parameters)
    trigger_read_accounts = TriggerDagRunOperator(
        task_id="trigger_read_accounts",
        trigger_dag_id="read_accounts",
        wait_for_completion=True,
        poke_interval=10,
        execution_date='{{ds}}',
        reset_dag_run=True
    )

    trigger_load_accounts_and_data_scrapping = TriggerDagRunOperator(
        task_id="load_accounts_and_data_scrapping",
        trigger_dag_id="load_accounts_and_data_scrapping",
        wait_for_completion=True,
        poke_interval=10,
        execution_date='{{ds}}',
        reset_dag_run=True
    )
        
    # Set dependencies
    trigger_read_accounts >> trigger_load_accounts_and_data_scrapping 
