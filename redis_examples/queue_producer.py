import argparse
import json
from uuid import uuid4

import redis


def build_task(raw_text: str) -> dict:
    payload = raw_text.strip()
    if payload.startswith("upper "):
        return {"id": str(uuid4()), "type": "upper", "payload": payload[6:]}
    if payload.startswith("reverse "):
        return {"id": str(uuid4()), "type": "reverse", "payload": payload[8:]}
    return {"id": str(uuid4()), "type": "echo", "payload": payload}


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis queue producer")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--queue", default="demo:tasks")
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    print(
        "Enter task text. Prefix with 'upper ' or 'reverse ' for handlers. "
        "Type 'exit' to stop."
    )

    while True:
        line = input("> ").strip()
        if not line:
            continue
        if line.lower() == "exit":
            print("Producer stopped.")
            break

        task = build_task(line)
        client.lpush(args.queue, json.dumps(task, ensure_ascii=True))
        print(f"queued: {task}")


if __name__ == "__main__":
    main()
