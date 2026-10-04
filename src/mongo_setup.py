"""تهيئة MongoDB وفحص المجموعات والفهارس."""

import sys
from pathlib import Path

from pymongo import MongoClient

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from config import settings  # noqa: E402


def get_client(timeout_ms: int | None = None) -> MongoClient:
    ms = timeout_ms if timeout_ms is not None else settings.MONGO_TIMEOUT_MS
    return MongoClient(
        settings.MONGO_URI,
        serverSelectionTimeoutMS=ms,
        connectTimeoutMS=ms,
        socketTimeoutMS=ms,
    )


def ping(client: MongoClient) -> None:
    client.admin.command("ping")


def collection_counts(db) -> dict:
    return {
        settings.COLLECTION_RAW: db[settings.COLLECTION_RAW].count_documents({}),
        settings.COLLECTION_VALIDATED: db[settings.COLLECTION_VALIDATED].count_documents({}),
        settings.COLLECTION_QUARANTINE: db[settings.COLLECTION_QUARANTINE].count_documents({}),
    }
