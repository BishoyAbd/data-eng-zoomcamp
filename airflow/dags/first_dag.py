from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'bishoykamel',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime.now(),  # Start date set to today
}

# Define the DAG using the `with` statement
with DAG(
    'bash_retry_dag',
    default_args=default_args,
    description='A simple DAG with BashOperator and retries',
    schedule_interval=None,  # Manually triggered or set to a specific interval
    catchup=False  # Disable backfilling
) as dag:

    # Define the BashOperator task
    hello_task = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello"',
    )

# The DAG and task are automatically associated due to the `with` statement
