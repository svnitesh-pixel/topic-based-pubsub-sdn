#!/usr/bin/env python3
"""Load generator for scalability / SDN experiments (R7, R8).

Many publishers or subscribers run as coroutines inside ONE process (one TCP
connection each), so hundreds of clients fit on a single Mininet host.

  loadgen.py sub --n 10 --topics sports news technology --out recv_h4.csv --stop-file STOP
  loadgen.py pub --n 10 --topics sports news technology --rate 100 --duration 20 --out sent_h1.csv

Latency = t4 - t1 (t1: publisher stamp, t4: subscriber receive stamp; all
Mininet hosts share one clock).
"""
import argparse
import asyncio
import csv
import json
import os
import random
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pubsub_common import (BROKER_PORT, MAX_LINE, decode, encode, port_for_topic,
                           raise_fd_limit, set_nodelay)


async def open_conn(host, port, retries=20):
    last = None
    for _ in range(retries):
        try:
            r, w = await asyncio.open_connection(host, port, limit=MAX_LINE)
            set_nodelay(w.get_extra_info("socket"))
            return r, w
        except OSError as exc:
            last = exc
            await asyncio.sleep(0.25)
    raise last


# ---------------------------------------------------------------- subscribers
async def run_sub(args):
    rows, assigned, ready = [], {}, [0]
    stop = asyncio.Event()

    async def one(i):
        gi = args.offset + i
        sid = f"{args.prefix}-sub-{i}"
        mine = [args.topics[(gi + k) % len(args.topics)] for k in range(args.span)]
        assigned[sid] = sorted(set(mine))
        r, w = await open_conn(args.broker, args.port)
        w.write(encode({"type": "HELLO", "id": sid, "role": "subscriber"}))
        w.write(encode({"type": "SUBSCRIBE", "topics": mine}))
        await w.drain()
        await r.readline()                       # wait for ACK
        ready[0] += 1
        try:
            while not stop.is_set():
                line = await r.readline()
                if not line:
                    break
                t4 = time.time()
                try:
                    m = decode(line)
                except ValueError:
                    continue
                if m.get("type") == "DELIVER":
                    rows.append((sid, m["msg_id"], m["topic"], m["ts"], m["t2"], m["t3"], t4))
        finally:
            w.close()

    tasks = [asyncio.ensure_future(one(i)) for i in range(args.n)]
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    deadline = time.time() + args.duration
    announced = False
    while not stop.is_set() and time.time() < deadline:
        if not announced and ready[0] >= args.n:
            announced = True
            if args.ready_file:
                Path(args.ready_file).write_text("ready")
        if args.stop_file and os.path.exists(args.stop_file):
            break
        await asyncio.sleep(0.2)
    stop.set()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    with open(args.out, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["sub_id", "msg_id", "topic", "t1", "t2", "t3", "t4"])
        wr.writerows(rows)
    Path(args.out).with_suffix(".subs.json").write_text(json.dumps(assigned))
    print(f"[loadgen sub {args.prefix}] {ready[0]}/{args.n} subscribed, "
          f"{len(rows)} messages received", flush=True)


# ------------------------------------------------------------------ publishers
async def run_pub(args):
    rows = []
    interval = args.n / args.rate                # per-publisher period (s)
    conns = []

    async def connect_one(i):
        gi = args.offset + i
        topic = args.topics[gi % len(args.topics)]
        r, w = await open_conn(args.broker, args.port or port_for_topic(topic))
        w.write(encode({"type": "HELLO", "id": f"{args.prefix}-pub-{i}", "role": "publisher"}))
        await w.drain()
        conns.append((i, topic, w))

    await asyncio.gather(*(connect_one(i) for i in range(args.n)))
    pad = "x" * max(0, args.size - 20)

    async def one(i, topic, w, start, end):
        pid = f"{args.prefix}-pub-{i}"
        nxt = start + random.random() * interval
        seq = 0
        while True:
            delay = nxt - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            if time.time() >= end:
                break
            seq += 1
            mid = f"{pid}-{seq}"
            t1 = time.time()
            try:
                w.write(encode({"type": "PUBLISH", "topic": topic, "msg_id": mid,
                                "ts": t1, "payload": pad}))
                await w.drain()
            except (ConnectionError, OSError):
                break
            rows.append((pid, mid, topic, t1))
            nxt += interval

    start = args.start_at or time.time()
    if start > time.time():
        await asyncio.sleep(start - time.time())
    end = start + args.duration
    await asyncio.gather(*(one(i, t, w, start, end) for i, t, w in conns))
    for _, _, w in conns:
        w.close()
    with open(args.out, "w", newline="") as f:
        wr = csv.writer(f)
        wr.writerow(["pub_id", "msg_id", "topic", "t1"])
        wr.writerows(rows)
    print(f"[loadgen pub {args.prefix}] {args.n} publishers sent {len(rows)} messages", flush=True)


def main():
    ap = argparse.ArgumentParser(description="pub-sub load generator")
    ap.add_argument("role", choices=["pub", "sub"])
    ap.add_argument("--broker", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=0,
                    help="pub: force one port (default: topic->port); sub: default 5000")
    ap.add_argument("--n", type=int, default=1, help="number of clients in this process")
    ap.add_argument("--topics", nargs="+", default=["sports", "news", "technology"])
    ap.add_argument("--span", type=int, default=1, help="sub: topics per subscriber")
    ap.add_argument("--offset", type=int, default=0, help="global index offset of first client")
    ap.add_argument("--prefix", default="h")
    ap.add_argument("--rate", type=float, default=100, help="pub: msgs/s of THIS process")
    ap.add_argument("--size", type=int, default=100, help="pub: approx message bytes")
    ap.add_argument("--duration", type=float, default=20, help="pub: send time; sub: max run time")
    ap.add_argument("--start-at", type=float, default=0, help="pub: epoch time to start sending")
    ap.add_argument("--out", required=True)
    ap.add_argument("--stop-file", help="sub: finish when this file appears")
    ap.add_argument("--ready-file", help="sub: created once all subscriptions are ACKed")
    args = ap.parse_args()
    raise_fd_limit()
    if args.role == "sub" and not args.port:
        args.port = BROKER_PORT
    asyncio.run(run_sub(args) if args.role == "sub" else run_pub(args))


if __name__ == "__main__":
    main()
