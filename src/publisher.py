#!/usr/bin/env python3
"""Publisher: plain blocking TCP socket client (R1).

The publisher opens a connection to the broker port that belongs to its topic
class (sports->5001, news->5002, technology->5003, otherwise 5000) so that the
SDN switches can classify the traffic by TCP destination port (R5).
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pubsub_common import connect, encode, port_for_topic


def main():
    ap = argparse.ArgumentParser(description="Pub-sub publisher")
    ap.add_argument("--broker", default="127.0.0.1", help="broker IP / hostname")
    ap.add_argument("--port", type=int, help="override the topic->port mapping")
    ap.add_argument("--id", default="pub-1")
    ap.add_argument("--topic", required=True)
    ap.add_argument("--message", default="hello")
    ap.add_argument("--count", type=int, default=1, help="messages to send (0 = forever)")
    ap.add_argument("--interval", type=float, default=0.0, help="seconds between messages")
    ap.add_argument("--size", type=int, default=0, help="pad payload to this many bytes")
    ap.add_argument("--retries", type=int, default=5, help="connect retries")
    args = ap.parse_args()

    port = args.port or port_for_topic(args.topic)
    payload = args.message
    if args.size > len(payload):
        payload = payload + "x" * (args.size - len(payload))

    def open_conn():
        s = connect(args.broker, port, retries=args.retries)
        s.sendall(encode({"type": "HELLO", "id": args.id, "role": "publisher"}))
        return s

    try:
        sock = open_conn()
    except OSError as exc:
        print(f"[{args.id}] cannot connect to {args.broker}:{port}: {exc}")
        return 1
    seq = 0
    try:
        while args.count == 0 or seq < args.count:
            seq += 1
            msg = {"type": "PUBLISH", "topic": args.topic, "msg_id": f"{args.id}-{seq}",
                   "ts": time.time(), "payload": payload}
            try:
                sock.sendall(encode(msg))
            except OSError:                       # connection lost: reconnect, retry once
                sock.close()
                sock = open_conn()
                msg["ts"] = time.time()
                sock.sendall(encode(msg))
            print(f"[{args.id}] sent id={msg['msg_id']} topic={args.topic} (port {port})",
                  flush=True)
            if args.interval:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        sock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
