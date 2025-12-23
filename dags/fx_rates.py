from airflow import DAG
from datetime import datetime, timedelta
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import requests, xmltodict, json, pathlib

default_args = {"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
        dag_id="fx_rates_daily",
        start_date=datetime(2025, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args=default_args,
        tags=["bootcamp-de"],
    ) as dag:


    @task(task_id="fetch_rates")
    def fetch_rates(run_dt: str):
        url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
        raw = requests.get(url, timeout=30).text
        data = xmltodict.parse(raw)
        out_dir = pathlib.Path("data",  run_dt)
        print(f"Writing FX rates to {out_dir}")
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "eurofx.json"
        with open(path, "w") as f:
            json.dump(data, f)
        return str(path)        # pushed to XCom

    starting_bash = BashOperator(
        task_id="starting_bash",
        bash_command="echo 'Starting FX Rates DAG for {{ ds }}'",
        )

    @task()
    def transform_rates(json_path: str, run_dt: str):
        import json, pandas as pd  # pandas not strictly needed, but left for later use
        with open(json_path) as f:
            raw = json.load(f)
        cubes = raw["gesmes:Envelope"]["Cube"]["Cube"]["Cube"]  # tiny XML shape
        rows = [
            {"base": "EUR", "quote": c["@currency"], "rate": c["@rate"]}
            for c in cubes
        ]
        return rows            # list of dicts -> XCom

    create_fx_table = SQLExecuteQueryOperator(
        task_id="create_fx_table",
        conn_id="my_postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS fx_rates (
            base_currency  text,
            quote_currency text,
            rate           numeric,
            run_date       date,
            PRIMARY KEY (quote_currency, run_date)
        );
        """,
    )

    load_fx = SQLExecuteQueryOperator(
        task_id="upsert_rates",
        conn_id="my_postgres",
        sql="""
        WITH data AS (
            SELECT *
            FROM json_to_recordset(%(rows_json)s::json)
                AS t(base text, quote text, rate numeric)
        )
        INSERT INTO fx_rates (base_currency, quote_currency, rate, run_date)
        SELECT base, quote, rate, %(run_dt)s::date
        FROM data
        ON CONFLICT (quote_currency, run_date) DO UPDATE
            SET rate          = EXCLUDED.rate,
                base_currency = EXCLUDED.base_currency;
        """,
        parameters={
            "rows_json": "{{ ti.xcom_pull(task_ids='transform_rates') | tojson }}",
            "run_dt": "{{ ds }}"
        },
    )

    audit = BashOperator(
        task_id="echo_count",
        bash_command="echo 'Inserted {{ ti.xcom_pull(task_ids='upsert_rates', key='return_value') }} rows for {{ ds }}'",
    )

    fetch_rates_task = fetch_rates(run_dt="{{ ds }}")
    transform_rates_task = transform_rates(json_path=fetch_rates_task, run_dt="{{ ds }}")

    starting_bash >> fetch_rates_task >> transform_rates_task >> create_fx_table >> load_fx >> audit