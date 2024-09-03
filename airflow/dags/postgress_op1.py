from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Default settings applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 8, 31),  # Adjust to the current date
}

# Define the DAG
with DAG(
    dag_id='create_and_insert_dag_runs',
    default_args=default_args,
    description='A DAG to create a dag_runs table and insert dag_id and execution date',
    schedule_interval=None,  # Run manually or set to a schedule like '@daily'
    catchup=False
) as dag:

    # Task 1: Create the table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS dag_runs (
        dt TIMESTAMP NOT NULL,
        dag_id VARCHAR(255) NOT NULL,
        PRIMARY KEY (dt, dag_id)
    );
    """

    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='pgdatabase_id',  # Ensure this connection is set up in Airflow
        sql=create_table_sql
    )

    # Task 2: Insert the dag_id and execution date into the table
    insert_dag_run_sql = """
    INSERT INTO dag_runs (dt, dag_id) VALUES ('{{ ds }}', '{{ dag.dag_id }}');
    """

    insert_dag_run = PostgresOperator(
        task_id='insert_dag_run',
        postgres_conn_id='pgdatabase_id',
        sql=insert_dag_run_sql,
    )

    # Set dependencies
    create_table >> insert_dag_run

