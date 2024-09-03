from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'bishoykamel',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime.now(),
}

# Function to append outputs
def append_output(ti):
    # Pull the output of the first task from XCom
    first_task_output = ti.xcom_pull(task_ids='echo_hello')
    
    # Define the second task's own output
    second_task_output = " World"
    
    # Combine the outputs
    combined_output = first_task_output + second_task_output
    
    # Print the combined output
    print(combined_output)
    
    # Optionally, return the combined output if another task needs it
    return combined_output

# Define the DAG using the `with` statement
with DAG(
    'bash_append_dag',
    default_args=default_args,
    description='A simple DAG that appends the output of two tasks',
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Echo "Hello"
    echo_hello = BashOperator(
        task_id='echo_hello',
        bash_command='echo "Hello"',
        do_xcom_push=True,  # Push the output to XCom
    )

    # Task 2: Append " World" to the output of Task 1
    append_world = PythonOperator(
        task_id='append_world',
        python_callable=append_output,
    )

    # Set task dependencies
    echo_hello >> append_world

# The DAG and tasks are automatically associated due to the `with` statement
