from __future__ import annotations

import random
import os
from datetime import UTC, datetime, timedelta

from pymongo import MongoClient

from mongo_orders_repository import OrdersRepository

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@localhost:27017/?authSource=admin")
DB_NAME = "training_db"
COLLECTION_NAME = "customer_orders"
SAMPLE_SIZE = 2000

COUNTRIES = ["US", "DE", "PL", "KZ", "UZ", "AE"]
CITIES = {
    "US": ["New York", "Austin", "Seattle"],
    "DE": ["Berlin", "Hamburg", "Munich"],
    "PL": ["Warsaw", "Krakow", "Gdansk"],
    "KZ": ["Almaty", "Astana", "Shymkent"],
    "UZ": ["Tashkent", "Samarkand", "Bukhara"],
    "AE": ["Dubai", "Abu Dhabi", "Sharjah"],
}
SEGMENTS = ["b2c", "b2b", "vip"]
CHANNELS = ["web", "mobile", "partner"]
STATUSES = ["new", "paid", "shipped", "delivered", "cancelled"]
CATEGORIES = ["electronics", "fashion", "home", "books", "fitness"]
SKUS = [f"SKU-{i:04d}" for i in range(1, 121)]


def build_order_document(index: int) -> dict:
    country = random.choice(COUNTRIES)
    city = random.choice(CITIES[country])
    channel = random.choice(CHANNELS)
    status = random.choices(STATUSES, weights=[20, 30, 20, 20, 10], k=1)[0]

    created_at = datetime.now(UTC) - timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    paid_at = created_at + timedelta(hours=random.randint(1, 72)) if status != "new" else None

    items = []
    total_amount = 0.0
    for _ in range(random.randint(1, 5)):
        qty = random.randint(1, 4)
        unit_price = round(random.uniform(10, 800), 2)
        discount_pct = random.choice([0, 0, 5, 10, 15])
        net_unit_price = round(unit_price * (1 - discount_pct / 100), 2)
        total_amount += qty * net_unit_price
        items.append(
            {
                "sku": random.choice(SKUS),
                "category": random.choice(CATEGORIES),
                "quantity": qty,
                "unit_price": unit_price,
                "net_unit_price": net_unit_price,
                "discount": {
                    "percent": discount_pct,
                    "campaign": "none" if discount_pct == 0 else random.choice(["flash", "bundle", "coupon"]),
                },
                "attributes": {
                    "color": random.choice(["black", "white", "blue", "green", "red"]),
                    "warehouse": random.choice(["w1", "w2", "w3"]),
                },
            }
        )

    shipping_cost = round(total_amount * random.uniform(0.01, 0.05), 2)
    grand_total = round(total_amount + shipping_cost, 2)

    return {
        "order_id": f"ORD-{index:08d}",
        "customer": {
            "id": f"CUS-{random.randint(1, 600):05d}",
            "profile": {
                "segment": random.choice(SEGMENTS),
                "contacts": {
                    "email": f"customer{random.randint(1, 600):05d}@example.com",
                    "phones": [f"+1-202-555-{random.randint(1000, 9999)}"],
                },
                "geo": {
                    "country": country,
                    "city": city,
                    "timezone": random.choice(["UTC", "Europe/Berlin", "Asia/Tashkent", "America/New_York"]),
                    "location": {
                        "lat": round(random.uniform(-90, 90), 6),
                        "lon": round(random.uniform(-180, 180), 6),
                    },
                },
                "preferences": {
                    "language": random.choice(["en", "de", "ru", "uz"]),
                    "marketing_opt_in": random.choice([True, False]),
                },
            },
        },
        "order": {
            "created_at": created_at,
            "status": status,
            "status_changed_at": paid_at or created_at,
            "channel": channel,
            "currency": "USD",
            "items": items,
            "total": {
                "subtotal": round(total_amount, 2),
                "shipping": shipping_cost,
                "amount": grand_total,
            },
            "payments": [
                {
                    "method": random.choice(["card", "wallet", "bank_transfer"]),
                    "paid_at": paid_at,
                    "provider": random.choice(["stripe", "adyen", "paypal"]),
                    "state": "confirmed" if paid_at else "pending",
                }
            ],
            "shipping": {
                "carrier": random.choice(["dhl", "ups", "fedex", "cdek"]),
                "promised_date": created_at + timedelta(days=random.randint(2, 12)),
                "delivered_date": created_at + timedelta(days=random.randint(3, 14)) if status == "delivered" else None,
                "address": {
                    "country": country,
                    "city": city,
                    "street": f"{random.randint(1, 200)} Main St",
                    "postal_code": f"{random.randint(10000, 99999)}",
                },
            },
            "timeline": [
                {
                    "at": created_at,
                    "status": "new",
                    "source": "checkout",
                }
            ],
            "notes": [],
        },
        "risk": {
            "score": random.randint(1, 100),
            "rules": {
                "fraud": {
                    "ip_velocity": random.randint(0, 50),
                    "device_reputation": random.choice(["good", "neutral", "bad"]),
                },
                "chargeback": {
                    "previous_disputes": random.randint(0, 5),
                    "country_mismatch": random.choice([True, False]),
                },
            },
        },
        "tags": random.sample(["priority", "gift", "returning", "first_order", "cross_border"], k=3),
        "meta": {
            "source": "seed_script",
            "schema_version": 1,
            "created_at": datetime.now(UTC),
        },
    }


def main() -> None:
    client = MongoClient(MONGO_URI)
    repo = OrdersRepository(client[DB_NAME], COLLECTION_NAME)

    docs = [build_order_document(i + 1) for i in range(SAMPLE_SIZE)]
    inserted_count = repo.replace_all(docs)

    print(f"Inserted documents: {inserted_count}")
    print(f"Database: {DB_NAME}")
    print(f"Collection: {COLLECTION_NAME}")
    print("Indexes:")
    for name in sorted(repo.collection.index_information().keys()):
        print(f"  - {name}")


if __name__ == "__main__":
    main()
