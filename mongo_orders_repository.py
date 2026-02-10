from __future__ import annotations

from datetime import datetime
from typing import Any

from pymongo import ASCENDING, DESCENDING
from pymongo.collection import Collection
from pymongo.database import Database


class OrdersRepository:
    def __init__(self, db: Database, collection_name: str = "customer_orders") -> None:
        self.collection: Collection = db[collection_name]

    def create_indexes(self) -> None:
        self.collection.create_index([("order_id", ASCENDING)], unique=True, name="ux_order_id")
        self.collection.create_index(
            [("order.created_at", DESCENDING), ("order.status", ASCENDING)],
            name="idx_created_status",
        )
        self.collection.create_index(
            [("customer.id", ASCENDING), ("order.created_at", DESCENDING)],
            name="idx_customer_created",
        )
        self.collection.create_index([("order.items.sku", ASCENDING)], name="idx_items_sku")
        self.collection.create_index(
            [
                ("customer.profile.geo.country", ASCENDING),
                ("order.channel", ASCENDING),
                ("order.created_at", DESCENDING),
            ],
            name="idx_country_channel_created",
        )

    def replace_all(self, documents: list[dict[str, Any]]) -> int:
        self.collection.drop()
        self.create_indexes()
        if not documents:
            return 0
        result = self.collection.insert_many(documents)
        return len(result.inserted_ids)

    def get_by_order_id(self, order_id: str) -> dict[str, Any] | None:
        return self.collection.find_one({"order_id": order_id}, {"_id": 0})

    def find_customer_orders(
        self,
        customer_id: str,
        created_from: datetime,
        created_to: datetime,
        statuses: list[str] | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        query: dict[str, Any] = {
            "customer.id": customer_id,
            "order.created_at": {"$gte": created_from, "$lte": created_to},
        }
        if statuses:
            query["order.status"] = {"$in": statuses}

        cursor = (
            self.collection.find(query, {"_id": 0})
            .hint("idx_customer_created")
            .sort("order.created_at", DESCENDING)
            .limit(limit)
        )
        return list(cursor)

    def set_order_status(self, order_id: str, new_status: str, changed_at: datetime) -> bool:
        result = self.collection.update_one(
            {"order_id": order_id},
            {
                "$set": {"order.status": new_status, "order.status_changed_at": changed_at},
                "$push": {
                    "order.timeline": {
                        "at": changed_at,
                        "status": new_status,
                        "source": "system",
                    }
                },
            },
        )
        return result.modified_count > 0

    def add_order_note(self, order_id: str, note: dict[str, Any]) -> bool:
        result = self.collection.update_one({"order_id": order_id}, {"$push": {"order.notes": note}})
        return result.modified_count > 0

    def gmv_by_day(
        self,
        created_from: datetime,
        created_to: datetime,
        statuses: list[str] | None = None,
        explain: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        match_filter: dict[str, Any] = {
            "order.created_at": {"$gte": created_from, "$lte": created_to},
        }
        if statuses:
            match_filter["order.status"] = {"$in": statuses}

        pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": {
                        "day": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$order.created_at",
                            }
                        },
                        "status": "$order.status",
                    },
                    "orders": {"$sum": 1},
                    "gmv": {"$sum": "$order.total.amount"},
                }
            },
            {"$sort": {"_id.day": 1, "_id.status": 1}},
        ]
        return self._run_aggregate_with_optional_explain(pipeline, "idx_created_status", explain)

    def top_skus_by_revenue(
        self,
        created_from: datetime,
        created_to: datetime,
        limit: int = 10,
        explain: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        pipeline = [
            {
                "$match": {
                    "order.created_at": {"$gte": created_from, "$lte": created_to},
                    "order.status": {"$in": ["paid", "shipped", "delivered"]},
                }
            },
            {"$unwind": "$order.items"},
            {
                "$group": {
                    "_id": "$order.items.sku",
                    "units": {"$sum": "$order.items.quantity"},
                    "revenue": {
                        "$sum": {
                            "$multiply": [
                                "$order.items.quantity",
                                "$order.items.unit_price",
                            ]
                        }
                    },
                }
            },
            {"$sort": {"revenue": -1}},
            {"$limit": limit},
        ]
        return self._run_aggregate_with_optional_explain(pipeline, "idx_created_status", explain)

    def country_channel_efficiency(
        self,
        created_from: datetime,
        created_to: datetime,
        country: str,
        explain: bool = False,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        pipeline = [
            {
                "$match": {
                    "customer.profile.geo.country": country,
                    "order.created_at": {"$gte": created_from, "$lte": created_to},
                }
            },
            {
                "$group": {
                    "_id": "$order.channel",
                    "orders": {"$sum": 1},
                    "avg_check": {"$avg": "$order.total.amount"},
                    "high_risk_share": {
                        "$avg": {
                            "$cond": [{"$gte": ["$risk.score", 70]}, 1, 0]
                        }
                    },
                }
            },
            {"$sort": {"orders": -1}},
        ]
        return self._run_aggregate_with_optional_explain(
            pipeline,
            "idx_country_channel_created",
            explain,
        )

    def _run_aggregate_with_optional_explain(
        self,
        pipeline: list[dict[str, Any]],
        hint_name: str,
        explain: bool,
    ) -> dict[str, Any] | list[dict[str, Any]]:
        if explain:
            return self.collection.database.command(
                {
                    "aggregate": self.collection.name,
                    "pipeline": pipeline,
                    "cursor": {},
                    "hint": hint_name,
                    "explain": True,
                }
            )

        return list(self.collection.aggregate(pipeline, hint=hint_name, allowDiskUse=True))

    @staticmethod
    def extract_used_indexes(explain_doc: dict[str, Any]) -> list[str]:
        indexes: set[str] = set()

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                stage = node.get("stage")
                index_name = node.get("indexName")
                if stage == "IXSCAN" and index_name:
                    indexes.add(index_name)
                for value in node.values():
                    walk(value)
                return

            if isinstance(node, list):
                for item in node:
                    walk(item)

        walk(explain_doc)
        return sorted(indexes)
