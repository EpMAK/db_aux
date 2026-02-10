from __future__ import annotations

import random
from datetime import datetime, timedelta, UTC

from pymongo import MongoClient

MONGO_URI = "mongodb://admin:admin@localhost:27017/?authSource=admin"
DB_NAME = "training_db"
COLLECTION_NAME = "events"
SAMPLE_SIZE = 100


def generate_document(index: int) -> dict:
    created_at = datetime.now(UTC) - timedelta(days=random.randint(0, 365))

    return {
        "record_id": index,
        "username": f"user_{index:03d}",
        "age": random.randint(18, 65),
        "score": round(random.uniform(0, 100), 2),
        "created_at": created_at,
        "is_active": random.choice([True, False]),
        "tags": random.sample(["alpha", "beta", "gamma", "delta", "omega"], k=3),
    }


def main() -> None:
    client = MongoClient(MONGO_URI)

    collection = client[DB_NAME][COLLECTION_NAME]
    collection.drop()

    docs = [generate_document(i + 1) for i in range(SAMPLE_SIZE)]
    result = collection.insert_many(docs)

    print(f"Inserted documents: {len(result.inserted_ids)}")
    print(f"Database: {DB_NAME}")
    print(f"Collection: {COLLECTION_NAME}")


if __name__ == "__main__":
    main()
