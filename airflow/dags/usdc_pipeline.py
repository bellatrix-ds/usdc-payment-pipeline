"""Hourly orchestration for the Base USDC payment pipeline."""

import logging
import os
import subprocess
from datetime import datetime, timedelta
from typing import Any

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


def alert_on_failure(context: dict[str, Any]) -> None:
    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["execution_date"]
    exception = context.get("exception", "Unknown error")
    logging.error(
        f"PIPELINE FAILURE | dag={dag_id} | task={task_id} | "
        f"execution_date={execution_date} | error={exception}"
    )


def get_block_range_for_last_hour() -> None:
    rpc_url = os.environ["ALCHEMY_BASE_RPC_URL"]
    response = requests.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"Alchemy RPC error: {payload['error']}")

    end_block = int(payload["result"], 16)
    start_block = end_block - 7200
    subprocess.run(
        [
            "python",
            "-m",
            "src.producer",
            "--start-block",
            str(start_block),
            "--end-block",
            str(end_block),
        ],
        cwd="/opt/airflow",
        check=True,
    )


def check_new_data() -> bool:
    host = os.environ["CLICKHOUSE_HOST"]
    port = os.environ["CLICKHOUSE_PORT"]
    user = os.environ["CLICKHOUSE_USER"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    query = (
        "SELECT count(*) FROM payments.raw_transfers "
        "WHERE block_timestamp >= now() - INTERVAL 2 HOUR"
    )
    response = requests.get(
        f"http://{host}:{port}/",
        params={"query": query},
        auth=(user, password),
        timeout=30,
    )
    response.raise_for_status()
    return int(response.text.strip()) > 0


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
}


with DAG(
    dag_id="usdc_pipeline",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
) as dag:
    fetch_transfers = PythonOperator(
        task_id="fetch_transfers",
        python_callable=get_block_range_for_last_hour,
        retries=3,
        retry_exponential_backoff=True,
        sla=timedelta(minutes=90),
    )

    wait_for_data = PythonSensor(
        task_id="wait_for_data",
        python_callable=check_new_data,
        poke_interval=30,
        timeout=600,
        sla=timedelta(minutes=90),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir .",
        sla=timedelta(minutes=30),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir .",
        on_failure_callback=alert_on_failure,
        sla=timedelta(minutes=10),
    )

    fetch_transfers >> wait_for_data >> dbt_run >> dbt_test
