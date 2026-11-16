#!/usr/bin/env python3
"""Pub-sub broker: asyncio TCP server, one coroutine per connection (R1,R2,R3,R4).

The broker listens on several ports at once (generic port + one port per topic
class).  Every port runs the same protocol; the *port* is what the SDN switches
can see, the *topic field* is what the broker reads (slide 8).
"""
import argparse
import asyncio
import csv
import logging
import signal
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from pubsub_common import (BROKER_PORT, MAX_LINE, TOPIC_PORTS, decode, encode,
                           raise_fd_limit, set_nodelay)

log = logging.getLogger("broker")


class Connection:
    """One TCP client. Outgoing data goes through a queue + sender task so a slow
    subscriber can never block the broker or a publisher (back-pressure safety)."""

    def __init__(self, cid, writer, queue_limit):
        self.id = cid
        self.writer = writer
        self.role = "unknown"
        self.via_port = 0
        self.queue = asyncio.Queue()
        self.queue_limit = queue_limit
        self.topics = set()
        self.task = asyncio.ensure_future(self._sender())

    def send(self, data: bytes) -> bool:
        if self.queue.qsize() >= self.queue_limit:
            return False
        self.queue.put_nowait(data)
        return True

    async def _sender(self):
        try:
            while True:
                data = await self.queue.get()
                if data is None:
                    break
                self.writer.write(data)
                await self.writer.drain()
        except (ConnectionError, OSError, asyncio.CancelledError):
            pass

    async def close(self):
        self.queue.put_nowait(None)
        try:
            await asyncio.wait_for(self.task, 1.0)
        except Exception:
            self.task.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


class Broker:
    def __init__(self, args):
        self.args = args
        self.topics = defaultdict(set)       # topic -> set[Connection]   (topic table)
        self.conns = set()
        self.stats = dict(connected=0, published=0, delivered=0,
                          no_match=0, slow_drops=0, errors=0)
        self.route_file = None
        self.route_csv = None
        if args.route_log:
            self.route_file = open(args.route_log, "w", newline="")
            self.route_csv = csv.writer(self.route_file)
            self.route_csv.writerow(["msg_id", "topic", "via_port", "t2", "t3", "n_subs"])

    # ------------------------------------------------------------------ I/O
    async def handle_client(self, reader, writer):
        peer = writer.get_extra_info("peername") or ("?", 0)
        local_port = (writer.get_extra_info("sockname") or ("?", 0))[1]
        set_nodelay(writer.get_extra_info("socket"))
        conn = Connection(f"{peer[0]}:{peer[1]}", writer, self.args.queue_limit)
        conn.via_port = local_port
        self.conns.add(conn)
        self.stats["connected"] += 1
        log.info("connected: %s (via port %d)", conn.id, local_port)
        try:
            while True:
                try:
                    line = await reader.readline()
                except ValueError:                       # line > MAX_LINE
                    self._error(conn, "message too large")
                    break
                if not line or not line.endswith(b"\n"):
                    break
                self.dispatch(conn, line, time.time())
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            pass
        finally:
            for t in list(conn.topics):
                self._unsubscribe(conn, t)
            self.conns.discard(conn)
            log.info("disconnected: %s", conn.id)
            await conn.close()

    # ------------------------------------------------------------- protocol
    def dispatch(self, conn, line, t2):
        try:
            msg = decode(line)
        except ValueError as exc:
            return self._error(conn, f"bad JSON: {exc}")
        mtype = msg.get("type")
        if mtype == "PUBLISH":
            return self._publish(conn, msg, t2)
        if mtype == "SUBSCRIBE":
            for t in self._topics_of(msg):
                self.topics[t].add(conn)
                conn.topics.add(t)
                log.info("%s SUBSCRIBE %s", conn.id, t)
            return conn.send(encode({"type": "ACK", "op": "SUBSCRIBE",
                                     "topics": sorted(conn.topics)}))
        if mtype == "UNSUBSCRIBE":
            for t in self._topics_of(msg):
                self._unsubscribe(conn, t)
                log.info("%s UNSUBSCRIBE %s", conn.id, t)
            return conn.send(encode({"type": "ACK", "op": "UNSUBSCRIBE",
                                     "topics": sorted(conn.topics)}))
        if mtype == "HELLO":
            conn.id = str(msg.get("id", conn.id))
            conn.role = str(msg.get("role", "unknown"))
            return None
        return self._error(conn, f"unknown type {mtype!r}")

    @staticmethod
    def _topics_of(msg):
        raw = msg.get("topics", msg.get("topic", []))
        if isinstance(raw, str):
            raw = [raw]
        return [t for t in raw if isinstance(t, str) and t]

    def _unsubscribe(self, conn, topic):
        conn.topics.discard(topic)
        subs = self.topics.get(topic)
        if subs is not None:
            subs.discard(conn)
            if not subs:
                del self.topics[topic]

    def _publish(self, conn, msg, t2):
        topic = msg.get("topic")
        if not isinstance(topic, str) or not topic:
            return self._error(conn, "PUBLISH needs a non-empty 'topic'")
        self.stats["published"] += 1
        subs = list(self.topics.get(topic, ()))
        t3 = time.time()
        if subs:
            data = encode({"type": "DELIVER", "topic": topic,
                           "msg_id": msg.get("msg_id"), "ts": msg.get("ts"),
                           "t2": t2, "t3": t3, "payload": msg.get("payload")})
            for s in subs:
                if s.send(data):
                    self.stats["delivered"] += 1
                else:
                    self.stats["slow_drops"] += 1
        else:
            self.stats["no_match"] += 1
        if self.route_csv:
            self.route_csv.writerow([msg.get("msg_id"), topic, conn.via_port,
                                     f"{t2:.6f}", f"{t3:.6f}", len(subs)])
        if log.isEnabledFor(logging.INFO):
            log.info("PUBLISH id=%s topic=%s via=:%d -> %s | routing latency = %.3f ms",
                     msg.get("msg_id"), topic, conn.via_port,
                     ",".join(sorted(s.id for s in subs)) or "none (dropped)",
                     (t3 - t2) * 1000)

    def _error(self, conn, text):
        self.stats["errors"] += 1
        log.warning("%s protocol error: %s", conn.id, text)
        conn.send(encode({"type": "ERROR", "error": text}))

    async def stats_printer(self, interval):
        while True:
            await asyncio.sleep(interval)
            s = self.stats
            print(f"[broker] clients={len(self.conns)} topics={len(self.topics)} "
                  f"published={s['published']} delivered={s['delivered']} "
                  f"no_match={s['no_match']} slow_drops={s['slow_drops']}", flush=True)

    def close_logs(self):
        if self.route_file:
            self.route_file.close()


async def run(args):
    raise_fd_limit()
    broker = Broker(args)
    servers = []
    for port in args.ports:
        servers.append(await asyncio.start_server(
            broker.handle_client, args.host, port, limit=MAX_LINE, backlog=2048))
    print(f"[broker] listening on {args.host} ports {args.ports} (asyncio)", flush=True)
    if args.stats_interval:
        asyncio.ensure_future(broker.stats_printer(args.stats_interval))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    for s in servers:
        s.close()
    for c in list(broker.conns):
        await c.close()
    broker.close_logs()
    print("[broker] stopped. stats:", broker.stats, flush=True)


def main():
    ap = argparse.ArgumentParser(description="Topic-based pub-sub broker (TCP, asyncio)")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--ports", type=int, nargs="+",
                    default=[BROKER_PORT] + sorted(TOPIC_PORTS.values()),
                    help="TCP ports to listen on (default 5000 5001 5002 5003)")
    ap.add_argument("--queue-limit", type=int, default=10000,
                    help="max queued messages per subscriber before dropping")
    ap.add_argument("--route-log", help="CSV file with t2/t3 per routed message")
    ap.add_argument("--stats-interval", type=float, default=0)
    ap.add_argument("--quiet", action="store_true", help="no per-message logging")
    args = ap.parse_args()
    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO,
                        format="[broker] %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
