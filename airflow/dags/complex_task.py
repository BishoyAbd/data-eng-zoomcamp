from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os

# Define default arguments
default_args = {
    'owner': 'bishoykamel',
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime.now(),
}

# Path to the temporary file
temp_file_path = '/tmp/airflow_temp_file.txt'

# Function to append a number to the file
def append_number_to_file(number, **kwargs):
    with open(temp_file_path, 'a') as f:
        f.write(f"{number}\n")
    print(f"Appended {number} to file.")

# Function to read and print the file's content
def print_file_content():
    if os.path.exists(temp_file_path):
        with open(temp_file_path, 'r') as f:
            content = f.read()
        print("File content:")
        print(content)
    else:
        print("File does not exist.")

# Define the DAG using the `with` statement
with DAG(
    'tree_structure_dag',
    default_args=default_args,
    description='A DAG that runs tasks in a tree structure and appends numbers to a file',
    schedule_interval=None,
    catchup=False
) as dag:

    # Level 1
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=append_number_to_file,
        op_kwargs={'number': 1},
    )

    # Level 2
    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=append_number_to_file,
        op_kwargs={'number': 2},
    )
    
    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=append_number_to_file,
        op_kwargs={'number': 3},
    )

    # Level 3
    task_4 = PythonOperator(
        task_id='task_4',
        python_callable=append_number_to_file,
        op_kwargs={'number': 4},
    )

    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=append_number_to_file,
        op_kwargs={'number': 5},
    )
    
    task_7 = PythonOperator(
        task_id='task_7',
        python_callable=append_number_to_file,
        op_kwargs={'number': 7},
    )

    task_6 = PythonOperator(
        task_id='task_6',
        python_callable=append_number_to_file,
        op_kwargs={'number': 6},
    )

    # Level 4
    task_8 = PythonOperator(
        task_id='task_8',
        python_callable=append_number_to_file,
        op_kwargs={'number': 8},
    )

    # Final task to print the file content
    final_task = PythonOperator(
        task_id='print_file_content',
        python_callable=print_file_content,
    )

    # Set up dependencies to create a tree structure
    task_1 >> [task_2, task_3]
    task_2 >> [task_4, task_5]
    task_3 >> [task_7, task_6]
    task_6 >> task_8

    # Final task depends on the last task in the tree
    task_8 >> final_task

# The DAG and tasks are automatically associated due to the `with` statement
