#!/usr/bin/env python3
"""Automated functional demo + verification inside Mininet (R1-R6).
Start the Ryu controller first (see README), then:
    sudo python3 mn/run_demo.py
Writes results/demo/* and prints PASS/FAIL for every check.
"""
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "mn"))
from mininet.log import setLogLevel          # noqa: E402
from topology import build_net               # noqa: E402

PY = sys.executable
OUT = ROOT / "results" / "demo"
checks = []


def check(name, ok, info=""):
    checks.append(ok)
    print(f"  [{'PASS' if ok else 'FAIL'}] {name} {info}")


def count(path, topic=None):
    if not os.path.exists(path):
        return 0
    n = 0
    for line in open(path).read().splitlines()[1:]:
        if topic is None or line.split(",")[2] == topic:
            n += 1
    return n


def main():
    setLogLevel("warning")
    OUT.mkdir(parents=True, exist_ok=True)
    for f in OUT.glob("*"):
        f.unlink()
    net = build_net()
    net.start()
    h1, h2, h3, h4, h5, h6 = (net.get(f"h{i}") for i in range(1, 7))
    time.sleep(3)
    print("\n== 1. SDN connectivity ==")
    check("h1 -> broker (h3) reachable via controller-installed flows",
          "1 received" in h1.cmd("ping -c1 -W2 10.0.0.3"))
    check("h4 -> broker (h3) reachable", "1 received" in h4.cmd("ping -c1 -W2 10.0.0.3"))

    print("\n== 2. start broker + subscribers ==")
    procs = []
    broker = h3.popen([PY, str(ROOT / "src/broker.py"), "--route-log", str(OUT / "broker_routes.csv")],
                      stdout=open(OUT / "broker.log", "w"), stderr=subprocess.STDOUT)
    procs.append(broker)
    time.sleep(1.5)
    spec = {h4: ("sub-1", ["sports", "news"]), h5: ("sub-2", ["news", "technology"]),
            h6: ("sub-3", ["sports"])}
    sub_p = {}
    for host, (sid, topics) in spec.items():
        p = host.popen([PY, str(ROOT / "src/subscriber.py"), "--broker", "10.0.0.3", "--id", sid,
                        "--topics", *topics, "--interactive", "--log", str(OUT / f"{sid}.csv")],
                       stdin=subprocess.PIPE, stdout=open(OUT / f"{sid}.out", "w"),
                       stderr=subprocess.STDOUT, text=True)
        sub_p[sid] = p
        procs.append(p)
    time.sleep(2)

    print("\n== 3. publish (topic -> TCP port class) ==")
    h1.cmd(f"{PY} {ROOT}/src/publisher.py --broker 10.0.0.3 --id pub-1 --topic sports "
           f"--message 'India won!' --count 5 --interval 0.1 > {OUT}/pub-1.out 2>&1")
    h2.cmd(f"{PY} {ROOT}/src/publisher.py --broker 10.0.0.3 --id pub-2 --topic technology "
           f"--message 'New release' --count 5 --interval 0.1 > {OUT}/pub-2.out 2>&1")
    time.sleep(1.5)
    check("sub-1 got 5 sports (subscribed sports,news)", count(OUT / "sub-1.csv", "sports") == 5)
    check("sub-3 got 5 sports", count(OUT / "sub-3.csv", "sports") == 5)
    check("sub-2 got 5 technology", count(OUT / "sub-2.csv", "technology") == 5)
    check("sub-1 got 0 technology (not subscribed)", count(OUT / "sub-1.csv", "technology") == 0)
    check("sub-2 got 0 sports (not subscribed)", count(OUT / "sub-2.csv", "sports") == 0)

    print("\n== 4. dynamic subscription change on a live connection (R2) ==")
    sub_p["sub-1"].stdin.write("unsub sports\nsub technology\n")
    sub_p["sub-1"].stdin.flush()
    time.sleep(1)
    before_s, before_t = count(OUT / "sub-1.csv", "sports"), count(OUT / "sub-1.csv", "technology")
    h1.cmd(f"{PY} {ROOT}/src/publisher.py --broker 10.0.0.3 --id pub-1b --topic sports --count 3 >/dev/null 2>&1")
    h2.cmd(f"{PY} {ROOT}/src/publisher.py --broker 10.0.0.3 --id pub-2b --topic technology --count 3 >/dev/null 2>&1")
    time.sleep(1.5)
    check("sub-1: no new sports after UNSUBSCRIBE", count(OUT / "sub-1.csv", "sports") == before_s)
    check("sub-1: 3 new technology after SUBSCRIBE", count(OUT / "sub-1.csv", "technology") == before_t + 3)

    print("\n== 5. SDN flow table evidence (R5) ==")
    flows = subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", "s1"],
                           capture_output=True, text=True).stdout
    (OUT / "flows_s1.txt").write_text(flows)
    for port, name in ((5001, "sports"), (5003, "technology")):
        m = re.search(rf"n_packets=(\d+).*tp_dst={port}", flows)
        check(f"s1 class rule tp_dst={port} ({name}) matched packets", bool(m) and int(m.group(1)) > 0,
              f"(n_packets={m.group(1) if m else 'n/a'})")
    print(flows)

    for p in procs:
        p.terminate()
    time.sleep(0.5)
    net.stop()
    print(f"\n{sum(checks)}/{len(checks)} checks passed. Logs: {OUT}")
    return 0 if all(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
