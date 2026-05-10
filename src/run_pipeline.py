import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.extract import extract_all
from src.load_bigquery import load_all
from src.freshness_checks import run_freshness_checks
from src.pipeline_log import insert_pipeline_run_log
from src.pipeline_health_summary import (
    create_or_replace_pipeline_health_summary,
    preview_pipeline_health_summary,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def run_pipeline() -> dict[str, Any]:
    pipeline_run_id = str(uuid4())
    pipeline_started_at = utc_now_iso()

    print("=" * 80)
    print("[PIPELINE] Starting API-to-BigQuery ingestion pipeline")
    print(f"[PIPELINE] Run ID: {pipeline_run_id}")
    print(f"[PIPELINE] Started at: {pipeline_started_at}")
    print("=" * 80)

    try:
        print("\n[STEP 1] Extracting data from API...")
        extraction_summary = extract_all()

        print("\n[STEP 2] Loading raw files into BigQuery...")
        load_summary = load_all()

        print("\n[STEP 3] Running freshness and row-count checks...")
        freshness_summary = run_freshness_checks()

        print("\n[STEP 4] Updating pipeline health summary...")
        create_or_replace_pipeline_health_summary()
        health_summary = preview_pipeline_health_summary()

        pipeline_finished_at = utc_now_iso()

        final_summary = {
            "pipeline_status": "success",
            "pipeline_run_id": pipeline_run_id,
            "pipeline_started_at": pipeline_started_at,
            "pipeline_finished_at": pipeline_finished_at,
            "extraction_summary": extraction_summary,
            "load_summary": load_summary,
            "freshness_summary": freshness_summary,
            "health_summary": health_summary,
        }

        print("\n" + "=" * 80)
        print("[PIPELINE] Completed successfully")
        print("=" * 80)
        print(json.dumps(final_summary, indent=2, default=str))

        return final_summary

    except Exception as exc:
        pipeline_finished_at = utc_now_iso()

        insert_pipeline_run_log(
            pipeline_run_id=pipeline_run_id,
            pipeline_name="api_to_bigquery_ingestion",
            started_at_utc=pipeline_started_at,
            finished_at_utc=pipeline_finished_at,
            status="failed",
            rows_products_loaded=0,
            rows_users_loaded=0,
            rows_carts_loaded=0,
            error_message=str(exc),
        )

        create_or_replace_pipeline_health_summary()
        health_summary = preview_pipeline_health_summary()

        failure_summary = {
            "pipeline_status": "failed",
            "pipeline_run_id": pipeline_run_id,
            "pipeline_started_at": pipeline_started_at,
            "pipeline_finished_at": pipeline_finished_at,
            "error_message": str(exc),
            "health_summary": health_summary,
        }

        print("\n" + "=" * 80)
        print("[PIPELINE] Failed and logged failure to BigQuery")
        print("=" * 80)
        print(json.dumps(failure_summary, indent=2, default=str))

        raise


if __name__ == "__main__":
    run_pipeline()