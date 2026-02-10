import argparse
import time

import redis


class RedisTTLCache:
    def __init__(self, redis_url: str) -> None:
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        self.client.setex(key, ttl_seconds, value)

    def get(self, key: str) -> str | None:
        return self.client.get(key)


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis TTL cache demo")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--key", default="demo:cache:key")
    parser.add_argument("--value", default="cached-value")
    parser.add_argument("--ttl", type=int, default=5)
    args = parser.parse_args()

    cache = RedisTTLCache(args.redis_url)
    cache.set(args.key, args.value, args.ttl)
    print(f"SET key={args.key!r}, value={args.value!r}, ttl={args.ttl}s")
    print(f"GET right after set: {cache.get(args.key)!r}")

    print(f"Sleeping {args.ttl + 1}s to wait for expiration...")
    time.sleep(args.ttl + 1)
    print(f"GET after expiration: {cache.get(args.key)!r}")


if __name__ == "__main__":
    main()
