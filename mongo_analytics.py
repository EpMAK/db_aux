from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient

from mongo_orders_repository import OrdersRepository

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017/?authSource=admin")
DB_NAME = "training_db"
COLLECTION_NAME = "customer_orders"


def print_aggregation_block(
    title: str,
    data: list[dict],
    explain_doc: dict,
) -> None:
    print(f"\n{title}")
    print("Results:")
    for row in data[:10]:
        print(f"  {row}")

    indexes = OrdersRepository.extract_used_indexes(explain_doc)
    print("Indexes used (from explain):", indexes if indexes else "NOT DETECTED")


def main() -> None:
    client = MongoClient(MONGO_URI)
    repo = OrdersRepository(client[DB_NAME], COLLECTION_NAME)

    end = datetime.now(UTC)
    start = end - timedelta(days=180)

    gmv_data = repo.gmv_by_day(start, end, statuses=["paid", "shipped", "delivered"], explain=False)
    gmv_explain = repo.gmv_by_day(start, end, statuses=["paid", "shipped", "delivered"], explain=True)
    print_aggregation_block("1) GMV по дням и статусам", gmv_data, gmv_explain)

    top_sku_data = repo.top_skus_by_revenue(start, end, limit=10, explain=False)
    top_sku_explain = repo.top_skus_by_revenue(start, end, limit=10, explain=True)
    print_aggregation_block("2) Топ SKU по выручке", top_sku_data, top_sku_explain)

    country_data = repo.country_channel_efficiency(start, end, country="US", explain=False)
    country_explain = repo.country_channel_efficiency(start, end, country="US", explain=True)
    print_aggregation_block("3) Эффективность каналов по стране", country_data, country_explain)


if __name__ == "__main__":
    main()
