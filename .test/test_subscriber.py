import argparse
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
from proto.queue_pb2 import RegisterRequest, UnregisterRequest
from proto.queue_pb2_grpc import MessageQueueStub


def make_handler(name: str):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            try:
                message = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError:
                message = {"raw": body.decode("utf-8", errors="replace")}
            print(f"{name} received | {json.dumps(message, ensure_ascii=False)}", flush=True)
            self.send_response(202)
            self.end_headers()

        def log_message(self, format, *args):
            return

    return Handler


def main():
    parser = argparse.ArgumentParser(description="Register an HTTP receiver with the broker")
    parser.add_argument("--broker", default="127.0.0.1:7500")
    parser.add_argument("--topic", default="cangling-test")
    parser.add_argument("--listen", default="127.0.0.1:8080")
    parser.add_argument("--callback-url", default="")
    parser.add_argument("--name", default="")
    parser.add_argument("--heartbeat", type=float, default=15.0)
    args = parser.parse_args()

    host, port_text = args.listen.rsplit(":", 1)
    port = int(port_text)
    callback_url = args.callback_url or f"http://{host}:{port}/messages"
    name = args.name or args.listen

    try:
        server = ThreadingHTTPServer((host, port), make_handler(name))
    except OSError as error:
        print(
            f"cannot listen on {host}:{port}: {error}\n"
            "that port is already taken (often `cargo run --example receiver`).\n"
            "stop the other process, or pick another port:\n"
            f"  python test_subscriber.py --topic {args.topic} --listen {host}:{port + 1} --name {name}",
            file=sys.stderr,
        )
        return 1
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    channel = grpc.insecure_channel(args.broker)
    stub = MessageQueueStub(channel)
    consumer_id = ""
    print(f"=== {name} listening {callback_url} ===")
    print(f"registering topic='{args.topic}' with broker {args.broker}")
    try:
        while True:
            response = stub.Register(
                RegisterRequest(
                    topic=args.topic,
                    downstream_url=callback_url,
                    consumer_id=consumer_id,
                ),
                timeout=5,
            )
            if response.consumer_id != consumer_id:
                consumer_id = response.consumer_id
                print(f"registered consumer_id={consumer_id}", flush=True)
            threading.Event().wait(args.heartbeat)
    except KeyboardInterrupt:
        print("\nStopping.")
    except grpc.RpcError as error:
        print(f"gRPC error: {error.code()} - {error.details()}", file=sys.stderr)
        return 1
    finally:
        if consumer_id:
            try:
                stub.Unregister(UnregisterRequest(consumer_id=consumer_id), timeout=5)
            except grpc.RpcError:
                pass
        server.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
