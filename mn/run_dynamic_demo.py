#!/usr/bin/env python3
"""Guided demo of dynamic SDN behaviour: congestion reroute, revert, link failure.
The Ryu controller must already be running.   sudo python3 mn/run_dynamic_demo.py"""
import csv
import json
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "mn"))
from mininet.log import setLogLevel   # noqa: E402
from topology import build_net        # noqa: E402

PY = sys.executable
OUT = ROOT / "results" / "dynamic_demo"
EVENTS = ROOT / "logs" / "controller_events.jsonl"
DEVNULL = subprocess.DEVNULL


def s1_path():
    out = subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "dump-flows", "s1"],
                         capture_output=True, text=True).stdout
    m = re.search(r'tp_dst=5001 actions=output:"?(?:s1-eth)?(\d+)', out)
    return {"3": "A via s2", "4": "B via s3"}.get(m.group(1), "?") if m else "?"


def window(path, t_from):
    lat = []
    try:
        with open(path) as f:
            for r in csv.DictReader(f):
                try:
                    if float(r["t4"]) >= t_from:
                        lat.append((float(r["t4"]) - float(r["t1"])) * 1000)
                except (ValueError, KeyError, TypeError):
                    pass
    except OSError:
        pass
    return lat


def new_events(state, t0):
    try:
        lines = EVENTS.read_text().splitlines()
    except OSError:
        return []
    out = []
    for line in lines[state[0]:]:
        try:
            e = json.loads(line)
        except ValueError:
            continue
        if e["t"] >= t0 and e["event"] in ("reroute", "port_status"):
            out.append(e)
    state[0] = len(lines)
    return out


def main():
    setLogLevel("warning")
    OUT.mkdir(parents=True, exist_ok=True)
    try:
        ev_state = [len(EVENTS.read_text().splitlines())]
    except OSError:
        ev_state = [0]
        print("WARNING: no", EVENTS, "- is the controller running from the project folder?")
    net = build_net()
    net.start()
    h = {i: net.get(f"h{i}") for i in range(1, 7)}
    procs = []
    try:
        time.sleep(3)
        if "1 received" not in h[1].cmd("ping -c1 -W2 10.0.0.3"):
            raise SystemExit("h1 cannot reach broker - is the controller running?")
        procs.append(h[3].popen([PY, str(ROOT / "src/broker.py"), "--quiet"], stdout=DEVNULL, stderr=DEVNULL))
        time.sleep(1.5)
        sublog = OUT / "sub1.csv"
        procs.append(h[4].popen([PY, str(ROOT / "src/subscriber.py"), "--broker", "10.0.0.3", "--id", "sub-1",
                                 "--topics", "sports", "--log", str(sublog)], stdout=DEVNULL, stderr=DEVNULL))
        time.sleep(1.5)
        procs.append(h[1].popen([PY, str(ROOT / "src/publisher.py"), "--broker", "10.0.0.3", "--id", "pub-1",
                                 "--topic", "sports", "--count", "0", "--interval", "0.05"],
                                stdout=DEVNULL, stderr=DEVNULL))
        T0 = time.time()
        actions = [
            (8, "FLOOD START: 9.5 Mbps UDP h2->h4 saturates Path A (s1-s2-s4) for 25 s"),
            (45, "LINK FAILURE: s2-s4 goes DOWN"),
            (60, "LINK RESTORED: s2-s4 goes UP"),
        ]
        done = set()
        rows = []
        print("\n  time | sports path (s1) | msgs/s | mean lat | max lat")
        while time.time() - T0 < 80:
            t = time.time() - T0
            for sec, label in actions:
                if t >= sec and sec not in done:
                    done.add(sec)
                    print(f"\n>>> t={t:4.0f}s  {label}")
                    if sec == 8:
                        procs.append(h[4].popen([PY, str(ROOT / "src/bgload.py"), "sink"], stdout=DEVNULL, stderr=DEVNULL))
                        procs.append(h[2].popen([PY, str(ROOT / "src/bgload.py"), "flood", "--dst", "10.0.0.4",
                                                 "--mbps", "9.5", "--duration", "25"], stdout=DEVNULL, stderr=DEVNULL))
                    elif sec == 45:
                        net.configLinkStatus("s2", "s4", "down")
                    elif sec == 60:
                        net.configLinkStatus("s2", "s4", "up")
            time.sleep(2)
            lat = window(sublog, time.time() - 2)
            mean = sum(lat) / len(lat) if lat else float("nan")
            mx = max(lat) if lat else float("nan")
            path = s1_path()
            print(f" {time.time() - T0:5.0f}s | {path:16s} | {len(lat) / 2:6.1f} | {mean:7.1f} ms | {mx:7.1f} ms")
            rows.append([round(time.time() - T0, 1), path, len(lat) / 2, round(mean, 2), round(mx, 2)])
            for e in new_events(ev_state, T0 - 1):
                if e["event"] == "reroute":
                    print(f"      CONTROLLER: reroute {e['topic']} {e['frm']}->{e['to']} reason={e['reason']}"
                          f"  (util A={e['util_A']}, B={e['util_B']})")
                else:
                    print(f"      CONTROLLER: port_status s{e['dpid']} port {e['port']} {e['state']}")
        with open(OUT / "timeline.csv", "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["t_s", "sports_path", "msgs_per_s", "mean_ms", "max_ms"])
            w.writerows(rows)
        print(f"\nSaved {OUT}/timeline.csv. Screenshot this output for the report.")
    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        net.stop()


if __name__ == "__main__":
    main()
