import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv
from google.cloud import bigquery

from src.config import PROJECT_ID, RAW_DATASET, RAW_DATA_DIR, validate_config
from src.pipeline_log import insert_pipeline_run_log

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


def get_latest_raw_file(entity_name: str) -> Path:
    files = list(RAW_DATA_DIR.glob(f"{entity_name}_*.json"))

    if not files:
        raise FileNotFoundError(f"No raw files found for entity: {entity_name}")

    return max(files, key=lambda path: path.stat().st_mtime)


def read_raw_payload(file_path: Path) -> dict[str, Any]:
    with file_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    required_keys = {"entity", "endpoint", "extracted_at_utc", "row_count", "records"}
    missing = required_keys - payload.keys()

    if missing:
        raise ValueError(f"Raw file {file_path} is missing keys: {missing}")

    if not isinstance(payload["records"], list):
        raise ValueError(f"Raw file {file_path} has invalid records structure")

    return payload


def normalize_records_for_bigquery(
    payload: dict[str, Any],
    source_file: Path,
    pipeline_run_id: str,
) -> list[dict[str, Any]]:
    loaded_at_utc = utc_now_iso()

    normalized_records = []

    for record in payload["records"]:
        normalized_record = {
            "raw_record": json.dumps(record, ensure_ascii=False),
            "_entity": payload["entity"],
            "_source_endpoint": payload["endpoint"],
            "_extracted_at_utc": payload["extracted_at_utc"],
            "_loaded_at_utc": loaded_at_utc,
            "_source_file": source_file.name,
            "_pipeline_run_id": pipeline_run_id,
        }

        normalized_records.append(normalized_record)

    return normalized_records


def load_records_to_bigquery(
    client: bigquery.Client,
    entity_name: str,
    records: list[dict[str, Any]],
) -> int:
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.raw_{entity_name}"

    schema = [
        bigquery.SchemaField("raw_record", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_entity", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_source_endpoint", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_extracted_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_loaded_at_utc", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("_source_file", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("_pipeline_run_id", "STRING", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    load_job = client.load_table_from_json(
        records,
        table_id,
        job_config=job_config,
    )

    load_job.result()

    destination_table = client.get_table(table_id)
    print(f"[OK] Loaded {len(records)} rows into {table_id}")
    print(f"[INFO] BigQuery table now has {destination_table.num_rows} total rows")

    return len(records)


def load_entity(entity_name: str, pipeline_run_id: str) -> int:
    client = get_bigquery_client()
    latest_file = get_latest_raw_file(entity_name)
    payload = read_raw_payload(latest_file)

    records = normalize_records_for_bigquery(
        payload=payload,
        source_file=latest_file,
        pipeline_run_id=pipeline_run_id,
    )

    return load_records_to_bigquery(
        client=client,
        entity_name=entity_name,
        records=records,
    )


def load_all() -> dict[str, Any]:
    pipeline_run_id = str(uuid4())
    started_at_utc = utc_now_iso()

    summary = {
        "pipeline_run_id": pipeline_run_id,
        "started_at_utc": started_at_utc,
        "entities": {},
    }

    try:
        for entity_name in ["products", "users", "carts"]:
            rows_loaded = load_entity(
                entity_name=entity_name,
                pipeline_run_id=pipeline_run_id,
            )

            summary["entities"][entity_name] = {
                "rows_loaded": rows_loaded,
            }

        finished_at_utc = utc_now_iso()

        insert_pipeline_run_log(
            pipeline_run_id=pipeline_run_id,
            pipeline_name="api_to_bigquery_ingestion",
            started_at_utc=started_at_utc,
            finished_at_utc=finished_at_utc,
            status="success",
            rows_products_loaded=summary["entities"]["products"]["rows_loaded"],
            rows_users_loaded=summary["entities"]["users"]["rows_loaded"],
            rows_carts_loaded=summary["entities"]["carts"]["rows_loaded"],
            error_message=None,
        )

        summary["finished_at_utc"] = finished_at_utc
        summary["status"] = "success"

        print(json.dumps(summary, indent=2))
        return summary

    except Exception as exc:
        finished_at_utc = utc_now_iso()

        insert_pipeline_run_log(
            pipeline_run_id=pipeline_run_id,
            pipeline_name="api_to_bigquery_ingestion",
            started_at_utc=started_at_utc,
            finished_at_utc=finished_at_utc,
            status="failed",
            rows_products_loaded=summary["entities"].get("products", {}).get("rows_loaded", 0),
            rows_users_loaded=summary["entities"].get("users", {}).get("rows_loaded", 0),
            rows_carts_loaded=summary["entities"].get("carts", {}).get("rows_loaded", 0),
            error_message=str(exc),
        )

        raise


if __name__ == "__main__":
    load_all()