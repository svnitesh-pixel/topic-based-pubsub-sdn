"""Functional tests that need NO Mininet: broker + clients on localhost.
Run:  python3 -m unittest tests.test_local -v     (from the project root)
Covers R1 (end to end), R2 (dynamic topics), R3 (concurrency), R4 (routing).
"""
import json
import socket
import subprocess
import sys
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
from pubsub_common import decode, encode  # noqa: E402

BASE = 15000


def free_ports(n=4):
    socks, ports = [], []
    for _ in range(n):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        socks.append(s)
        ports.append(s.getsockname()[1])
    for s in socks:
        s.close()
    return ports


class Client:
    def __init__(self, port, cid, role="subscriber"):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.buf = b""
        self.send({"type": "HELLO", "id": cid, "role": role})

    def send(self, m):
        self.sock.sendall(encode(m))

    def recv(self, timeout=2.0):
        self.sock.settimeout(timeout)
        while b"\n" not in self.buf:
            try:
                chunk = self.sock.recv(65536)
            except (socket.timeout, OSError):
                return None
            if not chunk:
                return None
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return decode(line)

    def drain(self, timeout=0.5):
        out = []
        while True:
            m = self.recv(timeout)
            if m is None:
                return out
            out.append(m)

    def close(self):
        self.sock.close()


class BrokerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ports = free_ports()
        cls.proc = subprocess.Popen(
            [sys.executable, str(ROOT / "src/broker.py"), "--quiet",
             "--ports", *map(str, cls.ports)], stdout=subprocess.DEVNULL)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", cls.ports[0]), 0.2).close()
                break
            except OSError:
                time.sleep(0.1)

    @classmethod
    def tearDownClass(cls):
        cls.proc.terminate()
        cls.proc.wait(5)

    def sub(self, cid, topics):
        c = Client(self.ports[0], cid)
        c.send({"type": "SUBSCRIBE", "topics": topics})
        self.assertEqual(c.recv()["type"], "ACK")
        return c

    def pub(self, cid, port_idx=1):
        return Client(self.ports[port_idx], cid, "publisher")

    def publish(self, p, topic, mid, payload="x"):
        p.send({"type": "PUBLISH", "topic": topic, "msg_id": mid,
                "ts": time.time(), "payload": payload})

    def test_routing_only_to_matching_subscribers(self):          # R1, R4
        s1, s2, s3 = (self.sub("s1", ["sports", "news"]),
                      self.sub("s2", ["news", "technology"]), self.sub("s3", ["sports"]))
        p = self.pub("p1")
        self.publish(p, "sports", "p1-1", "India won!")
        m1, m3 = s1.recv(), s3.recv()
        self.assertEqual((m1["type"], m1["topic"], m1["payload"]), ("DELIVER", "sports", "India won!"))
        self.assertEqual(m3["msg_id"], "p1-1")
        self.assertIsNone(s2.recv(0.5))                           # not subscribed
        self.assertIn("t2", m1)
        self.assertGreaterEqual(m1["t3"], m1["t2"])
        for c in (s1, s2, s3, p):
            c.close()

    def test_dynamic_subscribe_unsubscribe(self):                 # R2
        s = self.sub("dyn", ["sports"])
        p = self.pub("p2")
        self.publish(p, "sports", "a")
        self.assertEqual(s.recv()["msg_id"], "a")
        s.send({"type": "SUBSCRIBE", "topic": "technology"})
        s.send({"type": "UNSUBSCRIBE", "topic": "sports"})
        acks = [s.recv(), s.recv()]
        self.assertEqual(acks[1]["topics"], ["technology"])
        self.publish(p, "sports", "b")
        self.publish(p, "technology", "c")
        got = [m["msg_id"] for m in s.drain() if m["type"] == "DELIVER"]
        self.assertEqual(got, ["c"])                              # same connection, new topics
        s.close(); p.close()

    def test_no_subscribers_message_dropped_broker_survives(self):  # R4
        p = self.pub("p3")
        self.publish(p, "nobody-listens", "z")
        s = self.sub("late", ["nobody-listens"])
        self.assertIsNone(s.recv(0.5))                            # no replay for late subscribers
        self.publish(p, "nobody-listens", "z2")
        self.assertEqual(s.recv()["msg_id"], "z2")
        s.close(); p.close()

    def test_malformed_input_does_not_kill_broker(self):
        c = Client(self.ports[0], "bad")
        c.sock.sendall(b"this is not json\n")
        self.assertEqual(c.recv()["type"], "ERROR")
        c.send({"type": "NOPE"})
        self.assertEqual(c.recv()["type"], "ERROR")
        c.send({"type": "PUBLISH"})                               # missing topic
        self.assertEqual(c.recv()["type"], "ERROR")
        s = self.sub("after-bad", ["t"])                          # broker still serves
        s.close(); c.close()

    def test_abrupt_disconnect_cleans_topic_table(self):
        s = self.sub("gone", ["ghost"])
        s.sock.close()
        time.sleep(0.3)
        p = self.pub("p4")
        self.publish(p, "ghost", "g1")                            # must not raise / hang
        s2 = self.sub("alive", ["ghost"])
        self.publish(p, "ghost", "g2")
        self.assertEqual(s2.recv()["msg_id"], "g2")
        s2.close(); p.close()

    def test_topic_class_ports_all_serve_same_protocol(self):     # R5 (port per topic)
        s = self.sub("multi", ["sports", "news", "technology"])
        for idx, topic in ((1, "sports"), (2, "news"), (3, "technology")):
            p = self.pub(f"pp{idx}", idx)
            self.publish(p, topic, f"m-{topic}")
            self.assertEqual(s.recv()["topic"], topic)
            p.close()
        s.close()

    def test_concurrent_clients(self):                            # R3
        n_sub, n_pub, per_pub = 30, 20, 25
        subs = [self.sub(f"c{i}", ["load"]) for i in range(n_sub)]
        results = [0] * n_sub

        def reader(i):
            while True:
                m = subs[i].recv(2.0)
                if m is None:
                    return
                results[i] += 1

        def writer(i):
            p = self.pub(f"cp{i}")
            for k in range(per_pub):
                self.publish(p, "load", f"cp{i}-{k}")
            time.sleep(0.2)
            p.close()

        rt = [threading.Thread(target=reader, args=(i,)) for i in range(n_sub)]
        wt = [threading.Thread(target=writer, args=(i,)) for i in range(n_pub)]
        for t in rt + wt:
            t.start()
        for t in wt + rt:
            t.join()
        self.assertEqual(results, [n_pub * per_pub] * n_sub)      # no loss, no duplicates
        for c in subs:
            c.close()


if __name__ == "__main__":
    unittest.main()
