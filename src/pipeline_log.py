import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import PROJECT_ID, OPS_DATASET, validate_config

load_dotenv()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_bigquery_client() -> bigquery.Client:
    validate_config()

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_path:
        raise ValueError("Missing GOOGLE_APPLICATION_CREDENTIALS in .env")

    if not Path(credentials_path).exists():
        raise FileNotFoundError(f"Credentials file not found: {credentials_path}")

    return bigquery.Client(project=PROJECT_ID)


def get_pipeline_log_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("pipeline_run_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pipeline_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("started_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("finished_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rows_products_loaded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rows_users_loaded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("rows_carts_loaded", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
    ]


def ensure_pipeline_log_table(client: bigquery.Client, table_id: str) -> None:
    schema = get_pipeline_log_schema()
    table = bigquery.Table(table_id, schema=schema)

    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(table)
        print(f"[OK] Created table {table_id}")


def insert_pipeline_run_log(
    pipeline_run_id: str,
    pipeline_name: str,
    started_at_utc: str,
    finished_at_utc: str,
    status: str,
    rows_products_loaded: int = 0,
    rows_users_loaded: int = 0,
    rows_carts_loaded: int = 0,
    error_message: str | None = None,
) -> None:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{OPS_DATASET}.pipeline_run_log"

    ensure_pipeline_log_table(client, table_id)

    row = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_name": pipeline_name,
        "started_at_utc": started_at_utc,
        "finished_at_utc": finished_at_utc,
        "status": status,
        "rows_products_loaded": rows_products_loaded,
        "rows_users_loaded": rows_users_loaded,
        "rows_carts_loaded": rows_carts_loaded,
        "error_message": error_message,
    }

    job_config = bigquery.LoadJobConfig(
        schema=get_pipeline_log_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_json(
        [row],
        table_id,
        job_config=job_config,
    )

    load_job.result()

    print(f"[OK] Inserted pipeline run log into {table_id} using a BigQuery load job")
    print(json.dumps(row, indent=2))