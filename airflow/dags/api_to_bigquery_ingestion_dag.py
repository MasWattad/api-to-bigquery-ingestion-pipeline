from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = "/opt/airflow/api-to-bigquery-ingestion-pipeline"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt/api_ingestion_dbt"
DBT_PROFILES_DIR = f"{PROJECT_ROOT}/dbt"


with DAG(
    dag_id="api_to_bigquery_ingestion_pipeline",
    description=(
        "Ingest Fake Store API data into BigQuery, "
        "run freshness checks, transform with dbt, and validate outputs."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data-engineering", "bigquery", "dbt", "ingestion"],
) as dag:

    run_ingestion_pipeline = BashOperator(
        task_id="run_ingestion_pipeline",
        bash_command=f"cd {PROJECT_ROOT} && python -m src.run_pipeline",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    run_ingestion_pipeline >> dbt_run >> dbt_test