import argparse
import os
import sys

import grpc
from proto.queue_pb2 import AckMessageRequest, RegisterRequest, SubscribeRequest, UnregisterRequest
from proto.queue_pb2_grpc import MessageQueueStub


def auth_metadata(token):
    token = (token or "").strip()
    if not token:
        return None
    if not token.lower().startswith("bearer "):
        token = "Bearer " + token
    return [("authorization", token)]


def main():
    parser = argparse.ArgumentParser(description="Subscribe to the broker over a gRPC stream")
    parser.add_argument("--broker", default="127.0.0.1:7500")
    parser.add_argument("--topic", default="cangling-test")
    parser.add_argument("--name", default="python-subscriber")
    parser.add_argument("--consumer-id", default="")
    parser.add_argument("--token", default=os.environ.get("AUTH_TOKEN", ""))
    args = parser.parse_args()

    channel = grpc.insecure_channel(args.broker)
    stub = MessageQueueStub(channel)
    metadata = auth_metadata(args.token)
    consumer_id = args.consumer_id
    try:
        registered = stub.Register(
            RegisterRequest(topic=args.topic, consumer_id=consumer_id, name=args.name),
            timeout=5,
            metadata=metadata,
        )
        consumer_id = registered.consumer_id
        print(f"=== {args.name} registered consumer_id={consumer_id} ===")
        print(f"subscribing topic='{args.topic}' broker={args.broker}")
        for message in stub.Subscribe(
            SubscribeRequest(topic=args.topic, consumer_id=consumer_id),
            metadata=metadata,
        ):
            payload = message.payload.decode("utf-8", errors="replace")
            print(f"{args.name} received | {message.message_id} | {payload}", flush=True)
            stub.AckMessage(
                AckMessageRequest(
                    message_id=message.message_id,
                    lease=message.lease,
                    success=True,
                ),
                metadata=metadata,
            )
    except KeyboardInterrupt:
        print("\nStopping.")
    except grpc.RpcError as error:
        print(f"gRPC error: {error.code()} - {error.details()}", file=sys.stderr)
        return 1
    finally:
        if consumer_id:
            try:
                stub.Unregister(
                    UnregisterRequest(consumer_id=consumer_id),
                    timeout=5,
                    metadata=metadata,
                )
            except grpc.RpcError:
                pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
