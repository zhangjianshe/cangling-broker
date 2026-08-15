#!/usr/bin/env python3
import argparse
import os
import time

from cangling_message import SatwayClient, SubscribeOptions


def main() -> int:
    parser = argparse.ArgumentParser(description="Subscribe on a gRPC stream")
    parser.add_argument("--broker", default="127.0.0.1:7500")
    parser.add_argument("--topic", default="cangling-test")
    parser.add_argument("--name", default="python-consumer")
    parser.add_argument("--token", default=os.environ.get("AUTH_TOKEN", ""))
    args = parser.parse_args()

    def on_message(message):
        print(f"received | {message.id} | {message.topic} | {message.payload}", flush=True)

    with SatwayClient.connect(args.broker, args.token) as client:
        options = SubscribeOptions(topic=args.topic, name=args.name)
        with client.subscribe(options, on_message) as consumer:
            print(f"subscribed consumer_id={consumer.consumer_id}")
            print("Press Ctrl+C to stop.")
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nStopping.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
