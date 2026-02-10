import argparse
import json
import time

import redis


def handle_task(task: dict) -> str:
    task_type = task.get("type", "echo")
    payload = str(task.get("payload", ""))

    if task_type == "upper":
        return payload.upper()
    if task_type == "reverse":
        return payload[::-1]
    if task_type == "sleep":
        seconds = int(task.get("seconds", 1))
        time.sleep(max(0, seconds))
        return f"slept {seconds}s"
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis queue worker")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--queue", default="demo:tasks")
    parser.add_argument("--timeout", type=int, default=5)
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    print(f"Worker started. Waiting for tasks from {args.queue!r}...")

    try:
        while True:
            item = client.brpop(args.queue, timeout=args.timeout)
            if item is None:
                continue

            _, raw_task = item
            try:
                task = json.loads(raw_task)
            except json.JSONDecodeError:
                print(f"skip invalid task: {raw_task!r}")
                continue

            result = handle_task(task)
            print(f"processed id={task.get('id')} type={task.get('type')} result={result!r}")
    except KeyboardInterrupt:
        print("\nWorker stopped.")


if __name__ == "__main__":
    main()
