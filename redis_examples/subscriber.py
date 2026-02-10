import argparse

import redis


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis pub/sub subscriber")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--channel", default="demo:chat")
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    pubsub = client.pubsub()
    pubsub.subscribe(args.channel)

    print(f"Subscribed to {args.channel!r}. Waiting for messages...")
    try:
        for event in pubsub.listen():
            if event["type"] != "message":
                continue
            print(f"received: {event['data']}")
    except KeyboardInterrupt:
        print("\nSubscriber stopped.")
    finally:
        pubsub.close()


if __name__ == "__main__":
    main()
