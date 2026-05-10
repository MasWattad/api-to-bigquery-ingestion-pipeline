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


def create_or_replace_pipeline_health_summary() -> None:
    client = get_bigquery_client()

    view_id = f"{PROJECT_ID}.{OPS_DATASET}.pipeline_health_summary"

    query = f"""
    create or replace view `{view_id}` as

    with latest_run as (
      select
        pipeline_run_id,
        pipeline_name,
        started_at_utc,
        finished_at_utc,
        status,
        rows_products_loaded,
        rows_users_loaded,
        rows_carts_loaded,
        error_message
      from `{PROJECT_ID}.{OPS_DATASET}.pipeline_run_log`
      qualify row_number() over (order by finished_at_utc desc) = 1
    ),

    latest_freshness as (
      select
        table_name,
        latest_loaded_at_utc,
        row_count,
        freshness_status,
        checked_at_utc
      from `{PROJECT_ID}.{OPS_DATASET}.data_freshness_checks`
      qualify row_number() over (
        partition by table_name
        order by checked_at_utc desc
      ) = 1
    ),

    freshness_summary as (
      select
        count(*) as tables_checked,
        countif(freshness_status = 'fresh') as fresh_tables,
        countif(freshness_status != 'fresh') as unhealthy_tables,
        min(checked_at_utc) as earliest_check_time,
        max(checked_at_utc) as latest_check_time
      from latest_freshness
    )

    select
      r.pipeline_run_id,
      r.pipeline_name,
      r.started_at_utc,
      r.finished_at_utc,
      r.status as latest_pipeline_status,
      r.rows_products_loaded,
      r.rows_users_loaded,
      r.rows_carts_loaded,
      f.tables_checked,
      f.fresh_tables,
      f.unhealthy_tables,
      f.latest_check_time,
      case
        when r.status = 'success' and f.unhealthy_tables = 0 then 'healthy'
        when r.status != 'success' then 'pipeline_failed'
        when f.unhealthy_tables > 0 then 'freshness_issue'
        else 'unknown'
      end as overall_health_status,
      r.error_message
    from latest_run r
    cross join freshness_summary f
    """

    query_job = client.query(query)
    query_job.result()

    print(f"[OK] Created or replaced view: {view_id}")


def preview_pipeline_health_summary() -> list[dict]:
    client = get_bigquery_client()

    query = f"""
    select *
    from `{PROJECT_ID}.{OPS_DATASET}.pipeline_health_summary`
    """

    rows = list(client.query(query).result())
    results = [dict(row.items()) for row in rows]

    print(json.dumps(results, indent=2, default=str))
    return results


if __name__ == "__main__":
    create_or_replace_pipeline_health_summary()
    preview_pipeline_health_summary()