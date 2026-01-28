# dags/supplier_pipeline_dag.py
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Absolute path where your repo is mounted inside the Airflow container
REPO_ROOT = "/opt/airflow"

default_args = {
    "owner": "flavia",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def run_script(script_path: str) -> str:
    """
    Run a project script from the repo root.
    Airflow executes BashOperator commands from /tmp by default,
    so we explicitly cd into the mounted repo.
    """
    return f"bash -lc 'cd {REPO_ROOT} && python {script_path}'"


with DAG(
    dag_id="supplier_performance_pipeline",
    default_args=default_args,
    description="Run supplier performance pipeline (generate -> load -> KPIs -> risk)",
    start_date=datetime(2025, 1, 1),
    schedule="0 9 * * *",  # daily at 09:00
    catchup=False,
    tags=["capstone", "duckdb", "supplier-risk"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=run_script("src/generate_data.py"),
    )

    load_duckdb = BashOperator(
        task_id="load_duckdb",
        bash_command=run_script("src/load_duckdb.py"),
    )

    compute_kpis = BashOperator(
        task_id="compute_kpis",
        bash_command=run_script("src/compute_kpis.py"),
    )

    compute_risk = BashOperator(
        task_id="compute_risk",
        bash_command=run_script("src/compute_risk.py"),
    )

    generate_data >> load_duckdb >> compute_kpis >> compute_risk
