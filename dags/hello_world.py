from airflow import DAG
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="hello_world",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bootcamp-de"],
) as dag:

    @task()
    def say_hi():
        print("👋 Hello, Airflow!")

    show_date = BashOperator(
        task_id="show_date",
        bash_command="date",
    )

    say_hi() >> show_date