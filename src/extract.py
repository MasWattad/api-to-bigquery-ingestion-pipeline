import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from src.config import BASE_API_URL, ENDPOINTS, RAW_DATA_DIR, FORCE_API_FAILURE


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_file_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def fetch_endpoint(endpoint_path: str) -> list[dict[str, Any]]:
    url = f"{BASE_API_URL}{endpoint_path}"
    if FORCE_API_FAILURE and endpoint_path == "/products":
        url = f"{BASE_API_URL}/invalid-products-endpoint"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch data from {url}: {exc}") from exc

    data = response.json()

    if not isinstance(data, list):
        raise ValueError(f"Expected list response from {url}, got {type(data)}")

    if len(data) == 0:
        raise ValueError(f"Endpoint returned an empty response: {url}")

    return data


def save_raw_json(
    entity_name: str,
    endpoint_path: str,
    records: list[dict[str, Any]],
) -> Path:
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    extracted_at_utc = utc_now_iso()
    output_path = RAW_DATA_DIR / f"{entity_name}_{utc_file_timestamp()}.json"

    payload = {
        "entity": entity_name,
        "endpoint": endpoint_path,
        "extracted_at_utc": extracted_at_utc,
        "row_count": len(records),
        "records": records,
    }

    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)

    return output_path


def extract_all() -> dict[str, dict[str, Any]]:
    extraction_summary: dict[str, dict[str, Any]] = {}

    for entity_name, endpoint_path in ENDPOINTS.items():
        records = fetch_endpoint(endpoint_path)
        output_path = save_raw_json(entity_name, endpoint_path, records)

        extraction_summary[entity_name] = {
            "endpoint": endpoint_path,
            "rows_extracted": len(records),
            "raw_file": str(output_path),
        }

        print(
            f"[OK] Extracted {len(records)} rows "
            f"from {endpoint_path} into {output_path}"
        )

    return extraction_summary


if __name__ == "__main__":
    summary = extract_all()
    print(json.dumps(summary, indent=2))