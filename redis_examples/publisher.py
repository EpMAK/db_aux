import argparse

import redis


def main() -> None:
    parser = argparse.ArgumentParser(description="Redis pub/sub publisher")
    parser.add_argument("--redis-url", default="redis://localhost:6379/0")
    parser.add_argument("--channel", default="demo:chat")
    args = parser.parse_args()

    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    print(f"Publishing to channel {args.channel!r}. Type 'exit' to stop.")

    while True:
        message = input("> ").strip()
        if not message:
            continue
        if message.lower() == "exit":
            print("Publisher stopped.")
            break

        receivers = client.publish(args.channel, message)
        print(f"sent={message!r}, subscribers={receivers}")


if __name__ == "__main__":
    main()
