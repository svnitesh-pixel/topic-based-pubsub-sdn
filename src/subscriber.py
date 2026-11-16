#!/usr/bin/env python3
"""Subscriber: plain blocking TCP socket client (R1, R2, R7).

Topics can be changed while connected with --interactive (stdin commands
'sub <topic>', 'unsub <topic>', 'quit').  Every DELIVER is timestamped (t4) and
optionally logged to CSV so latency = t4 - t1 can be computed.
"""
import argparse
import csv
import socket
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pubsub_common import BROKER_PORT, connect, decode, encode


def _shutdown(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def main():
    ap = argparse.ArgumentParser(description="Pub-sub subscriber")
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=BROKER_PORT)
    ap.add_argument("--id", default="sub-1")
    ap.add_argument("--topics", nargs="*", default=[])
    ap.add_argument("--count", type=int, default=0, help="exit after N messages (0 = never)")
    ap.add_argument("--duration", type=float, default=0, help="exit after N seconds (0 = never)")
    ap.add_argument("--log", help="CSV file: sub_id,msg_id,topic,t1,t2,t3,t4")
    ap.add_argument("--interactive", action="store_true",
                    help="read 'sub X' / 'unsub X' commands from stdin")
    ap.add_argument("--retries", type=int, default=10)
    args = ap.parse_args()

    try:
        sock = connect(args.broker, args.port, retries=args.retries)
    except OSError as exc:
        print(f"[{args.id}] cannot connect to {args.broker}:{args.port}: {exc}")
        return 1
    print(f"[{args.id}] connected to broker {args.broker}:{args.port}", flush=True)
    lock = threading.Lock()

    def send(msg):
        with lock:
            sock.sendall(encode(msg))

    send({"type": "HELLO", "id": args.id, "role": "subscriber"})
    if args.topics:
        send({"type": "SUBSCRIBE", "topics": args.topics})

    writer = logf = None
    if args.log:
        logf = open(args.log, "w", newline="")
        writer = csv.writer(logf)
        writer.writerow(["sub_id", "msg_id", "topic", "t1", "t2", "t3", "t4"])

    if args.duration:
        threading.Timer(args.duration, lambda: _shutdown(sock)).start()

    if args.interactive:
        def stdin_loop():
            for cmd in sys.stdin:
                parts = cmd.split()
                if not parts:
                    continue
                try:
                    if parts[0] == "sub" and len(parts) > 1:
                        send({"type": "SUBSCRIBE", "topics": parts[1:]})
                    elif parts[0] == "unsub" and len(parts) > 1:
                        send({"type": "UNSUBSCRIBE", "topics": parts[1:]})
                    elif parts[0] == "quit":
                        _shutdown(sock)
                        return
                except OSError:
                    return
        threading.Thread(target=stdin_loop, daemon=True).start()

    received = 0
    try:
        for line in sock.makefile("rb"):
            t4 = time.time()
            try:
                msg = decode(line)
            except ValueError:
                continue
            kind = msg.get("type")
            if kind == "DELIVER":
                received += 1
                lat = (t4 - msg["ts"]) * 1000 if msg.get("ts") else float("nan")
                print(f"[{args.id}] RECEIVED id={msg['msg_id']} topic={msg['topic']} "
                      f"payload={msg['payload']!r}\n         latency = Rx - Tx = {lat:.2f} ms",
                      flush=True)
                if writer:
                    writer.writerow([args.id, msg["msg_id"], msg["topic"], msg["ts"],
                                     msg["t2"], msg["t3"], f"{t4:.6f}"])
                    logf.flush()
                if args.count and received >= args.count:
                    break
            elif kind == "ACK":
                print(f"[{args.id}] {msg['op']} ok -> now subscribed: "
                      f"{', '.join(msg['topics']) or '(none)'}", flush=True)
            elif kind == "ERROR":
                print(f"[{args.id}] broker error: {msg.get('error')}", flush=True)
    except (OSError, KeyboardInterrupt):
        pass
    finally:
        if logf:
            logf.close()
        sock.close()
    print(f"[{args.id}] done, received {received} messages", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
