import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import PROJECT_ID, RAW_DATASET, OPS_DATASET, validate_config

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


def get_freshness_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("check_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("latest_loaded_at_utc", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("row_count", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("freshness_status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("checked_at_utc", "TIMESTAMP", mode="REQUIRED"),
    ]


def ensure_freshness_table(client: bigquery.Client, table_id: str) -> None:
    table = bigquery.Table(table_id, schema=get_freshness_schema())

    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(table)
        print(f"[OK] Created table {table_id}")


def run_table_check(client: bigquery.Client, raw_table_name: str) -> dict:
    full_table_id = f"{PROJECT_ID}.{RAW_DATASET}.{raw_table_name}"

    query = f"""
    SELECT
      COUNT(*) AS row_count,
      MAX(_loaded_at_utc) AS latest_loaded_at_utc
    FROM `{full_table_id}`
    """

    result = list(client.query(query).result())[0]

    row_count = result["row_count"]
    latest_loaded_at_utc = result["latest_loaded_at_utc"]

    if row_count == 0:
        freshness_status = "empty"
    elif latest_loaded_at_utc is None:
        freshness_status = "missing_loaded_timestamp"
    else:
        freshness_status = "fresh"

    return {
        "check_id": f"{raw_table_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}",
        "table_name": raw_table_name,
        "latest_loaded_at_utc": latest_loaded_at_utc.isoformat() if latest_loaded_at_utc else None,
        "row_count": row_count,
        "freshness_status": freshness_status,
        "checked_at_utc": utc_now_iso(),
    }


def run_freshness_checks() -> list[dict]:
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{OPS_DATASET}.data_freshness_checks"

    ensure_freshness_table(client, table_id)

    raw_tables = ["raw_products", "raw_users", "raw_carts"]
    check_rows = []

    for raw_table in raw_tables:
        check_row = run_table_check(client, raw_table)
        check_rows.append(check_row)

    job_config = bigquery.LoadJobConfig(
        schema=get_freshness_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    load_job = client.load_table_from_json(
        check_rows,
        table_id,
        job_config=job_config,
    )
    load_job.result()

    print(f"[OK] Inserted {len(check_rows)} freshness check rows into {table_id}")
    print(json.dumps(check_rows, indent=2))

    return check_rows


if __name__ == "__main__":
    run_freshness_checks()