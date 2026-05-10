import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

BASE_API_URL = "https://fakestoreapi.com"

ENDPOINTS = {
    "products": "/products",
    "users": "/users",
    "carts": "/carts",
}

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

PROJECT_ID = os.getenv("PROJECT_ID")
LOCATION = os.getenv("LOCATION", "US")

RAW_DATASET = os.getenv("RAW_DATASET", "api_raw")
STG_DATASET = os.getenv("STG_DATASET", "api_stg")
MART_DATASET = os.getenv("MART_DATASET", "api_mart")
OPS_DATASET = os.getenv("OPS_DATASET", "api_ops")
FORCE_API_FAILURE = os.getenv("FORCE_API_FAILURE", "false").lower() == "true"

def validate_config() -> None:
    missing = []

    if not PROJECT_ID:
        missing.append("PROJECT_ID")

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )