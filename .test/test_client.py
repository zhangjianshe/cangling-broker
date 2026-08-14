import argparse
import sys
import time

import grpc
from proto.queue_pb2 import AcceptMessageRequest, AcceptMessageResponse
from proto.queue_pb2_grpc import MessageQueueStub


class TestClient:
    def __init__(self, server_addr="127.0.0.1:7500"):
        self.server_addr = server_addr
        self.channel = grpc.insecure_channel(server_addr)
        self.stub = MessageQueueStub(self.channel)

    def send_message(self, topic, payload, idempotency_key=None, attributes=None):
        if attributes is None:
            attributes = {}
        request = AcceptMessageRequest(
            idempotency_key=idempotency_key or "",
            topic=topic,
            payload=payload.encode("utf-8") if isinstance(payload, str) else payload,
            attributes=attributes,
        )
        try:
            response: AcceptMessageResponse = self.stub.AcceptMessage(request, timeout=5)
            print(f"Success | ID: {response.message_id} | Duplicate: {response.duplicate}")
            return response.message_id
        except grpc.RpcError as e:
            print(f"gRPC error: {e.code()} - {e.details()}")
            return None


def main():
    parser = argparse.ArgumentParser(description="Publish messages to cangling-message")
    parser.add_argument("--broker", default="127.0.0.1:7500")
    parser.add_argument("--topic", default="cangling-test")
    parser.add_argument("--text", default="")
    parser.add_argument("--count", type=int, default=0, help="send this many times then exit; 0 = loop")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    client = TestClient(args.broker)
    text = args.text.strip()
    if not text:
        try:
            text = input("Enter text to send (or leave empty to quit): ").strip()
        except EOFError:
            text = ""
    if not text:
        print("Exiting.")
        return 0

    print("=== cangling-message Python Test Client ===")
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            if client.send_message(args.topic, text) is None:
                return 1
            sent += 1
            if args.count == 0 or sent < args.count:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
