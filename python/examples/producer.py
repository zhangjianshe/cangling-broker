#!/usr/bin/env python3
import argparse
import os

from cangling_message import SatwayClient


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish messages on AcceptMessages")
    parser.add_argument("--broker", default="127.0.0.1:7500")
    parser.add_argument("--topic", default="cangling-test")
    parser.add_argument("--text", default="hello")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--token", default=os.environ.get("AUTH_TOKEN", ""))
    args = parser.parse_args()

    with SatwayClient.connect(args.broker, args.token) as client:
        for i in range(args.count):
            payload = args.text if args.count == 1 else f"{args.text}-{i}"
            result = client.send(args.topic, payload)
            print(f"sent {result}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
