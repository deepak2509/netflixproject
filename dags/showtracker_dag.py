from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='show_tracker_etl_dag',
    description='ETL pipeline for Streaming Show Performance Tracker',
    default_args=default_args,
    start_date=datetime(2024, 6, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['ETL', 'streaming', 'show-tracker'],
) as dag:

    extract_netflix = BashOperator(
        task_id='extract_netflix',
        bash_command='python3 /opt/airflow/scripts/extract/netflix_extract.py'
    )

    extract_youtube = BashOperator(
        task_id='extract_youtube',
        bash_command='python3 /opt/airflow/scripts/extract/youtube.py'
    )

    extract_imdb = BashOperator(
        task_id='extract_imdb',
        bash_command='python3 /opt/airflow/scripts/extract/imdbscrapper.py'
    )

    extract_reddit = BashOperator(
        task_id='extract_reddit',
        bash_command='python3 /opt/airflow/scripts/extract/reddit.py'
    )

    extract_google_trends = BashOperator(
        task_id='extract_google_trends',
        bash_command='python3 /opt/airflow/scripts/extract/google_trends.py'
    )

    transform_data = BashOperator(
        task_id='transform_merge_data',
        bash_command='python3 /opt/airflow/scripts/transform/transform.py'
    )

    load_data = BashOperator(
        task_id='load_to_s3',
        bash_command='python3 /opt/airflow/scripts/load/loadtos3.py'
    )

    # Set sequential order of tasks
    extract_netflix >> extract_youtube >> extract_imdb >> extract_reddit >> extract_google_trends >> transform_data >> load_data
