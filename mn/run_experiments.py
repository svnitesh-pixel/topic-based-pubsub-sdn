#!/usr/bin/env python3
"""Experiment runner: STATIC baseline vs DYNAMIC SDN, plus scalability (R6, R7, R8).

  sudo python3 mn/run_experiments.py compare   --ryu-cmd ryu-manager
  sudo python3 mn/run_experiments.py scale     --ryu-cmd ryu-manager [--quick]
  sudo python3 mn/run_experiments.py all

compare: scenarios normal | congestion | failure   x   modes static | dynamic
  congestion: UDP flood h2->h4 (9.5 Mbps) saturates Path A (s1-s2-s4)
  failure   : link s2-s4 goes down, later up again
scale   : E1 publishers, E2 subscribers, E3 connections, E4 topics, E5 msg rate
The runner starts/stops the Ryu controller itself so each run begins from a clean state.
"""
import argparse
import csv
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "mn"))
sys.path.insert(0, str(ROOT / "analysis"))
from mininet.log import setLogLevel   # noqa: E402
from topology import build_net        # noqa: E402
from analyze import analyze           # noqa: E402

try:
    import psutil
except ImportError:
    psutil = None

PY = sys.executable
BROKER_IP = "10.0.0.3"
TOPICS3 = ["sports", "news", "technology"]


def topic_list(n):
    return TOPICS3[:n] if n <= 3 else TOPICS3 + [f"topic{i}" for i in range(4, n + 1)]


def start_controller(ryu_cmd, mode, logdir, bw):
    env = dict(os.environ, PUBSUB_MODE=mode, PUBSUB_LOGDIR=str(logdir), LINK_BW_MBPS=str(bw))
    log = open(Path(logdir) / "ryu.log", "w")
    p = subprocess.Popen(ryu_cmd.split() + [str(ROOT / "sdn/ryu_pubsub_controller.py")],
                         stdout=log, stderr=subprocess.STDOUT, env=env, preexec_fn=os.setsid)
    time.sleep(4)
    if p.poll() is not None:
        raise SystemExit(f"Ryu failed to start, see {logdir}/ryu.log")
    return p


def stop(p, sig=signal.SIGTERM):
    if p and p.poll() is None:
        try:
            os.killpg(os.getpgid(p.pid), sig)
        except ProcessLookupError:
            pass
        try:
            p.wait(5)
        except subprocess.TimeoutExpired:
            p.kill()


def run_one(out, mode, spec, scenario, ryu_cmd, args):
    """One experiment: fresh controller + fresh Mininet + broker + load."""
    out = Path(out)
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    ctrl = start_controller(ryu_cmd, mode, out, args.core_bw)
    net = build_net(core_bw=args.core_bw)
    h = {}
    broker = None
    events = {}
    procs = []
    bg = []
    try:
        net.start()
        h = {i: net.get(f"h{i}") for i in range(1, 7)}
        try:
            net.waitConnected(timeout=15)
        except Exception:
            pass
        time.sleep(3)
        if "1 received" not in h[1].cmd(f"ping -c1 -W2 {BROKER_IP}"):
            raise RuntimeError("h1 cannot reach broker: is the controller working? see ryu.log")
        topics = topic_list(spec["topics"])
        broker = h[3].popen([PY, str(ROOT / "src/broker.py"), "--quiet",
                             "--route-log", str(out / "broker_routes.csv")],
                            stdout=open(out / "broker.log", "w"), stderr=subprocess.STDOUT)
        time.sleep(1.5)
        stopf = out / "STOP"
        # subscribers: spread over h4, h5, h6
        subs_hosts = [h[4], h[5], h[6]]
        n_sub, off = spec["subs"], 0
        ready = []
        for k, host in enumerate(subs_hosts):
            n = n_sub // 3 + (1 if k < n_sub % 3 else 0)
            if n == 0:
                continue
            rf = out / f"ready_{k}"
            ready.append(rf)
            procs.append(host.popen(
                [PY, str(ROOT / "src/loadgen.py"), "sub", "--broker", BROKER_IP, "--n", str(n),
                 "--offset", str(off), "--prefix", f"h{4 + k}", "--topics", *topics,
                 "--out", str(out / f"recv_h{4 + k}.csv"), "--stop-file", str(stopf),
                 "--ready-file", str(rf), "--duration", str(spec["duration"] + 90)],
                stdout=open(out / f"sub_h{4 + k}.log", "w"), stderr=subprocess.STDOUT))
            off += n
        t_end = time.time() + 60
        while time.time() < t_end and not all(r.exists() for r in ready):
            time.sleep(0.3)
        if not all(r.exists() for r in ready):
            raise RuntimeError("subscribers did not finish subscribing (see sub_h*.log)")
        # publishers: split over h1, h2
        start_at = time.time() + 3 + spec["pubs"] / 40.0
        pub_p = []
        n_pub = spec["pubs"]
        for k, host in enumerate((h[1], h[2])):
            n = n_pub // 2 + (1 if k < n_pub % 2 else 0)
            if n == 0:
                continue
            pub_p.append(host.popen(
                [PY, str(ROOT / "src/loadgen.py"), "pub", "--broker", BROKER_IP, "--n", str(n),
                 "--offset", str(k * (n_pub // 2 + n_pub % 2)), "--prefix", f"h{1 + k}",
                 "--topics", *topics, "--rate", str(spec["rate"] * n / n_pub),
                 "--duration", str(spec["duration"]), "--start-at", str(start_at),
                 "--size", str(spec.get("size", 100)), "--out", str(out / f"sent_h{1 + k}.csv")],
                stdout=open(out / f"pub_h{1 + k}.log", "w"), stderr=subprocess.STDOUT))
        cpu0 = None
        proc = psutil.Process(broker.pid) if psutil else None
        time.sleep(max(0, start_at - time.time()))
        if proc:
            ct = proc.cpu_times()
            cpu0 = (ct.user + ct.system, time.time())
        T = start_at
        # ---- scenario events (times relative to start of publishing)
        if scenario == "congestion":
            time.sleep(max(0, T + args.event_at - time.time()))
            bg.append(h[4].popen([PY, str(ROOT / "src/bgload.py"), "sink"]))
            bg.append(h[2].popen([PY, str(ROOT / "src/bgload.py"), "flood", "--dst", "10.0.0.4",
                                     "--mbps", str(args.core_bw * 0.95), "--duration", str(args.event_len)]))
            events.update(bg_start=time.time(), bg_end=time.time() + args.event_len)
        elif scenario == "failure":
            time.sleep(max(0, T + args.event_at - time.time()))
            net.configLinkStatus("s2", "s4", "down")
            events["link_down"] = time.time()
            time.sleep(args.event_len)
            net.configLinkStatus("s2", "s4", "up")
            events["link_up"] = time.time()
        for p in pub_p:
            p.wait(timeout=spec["duration"] + 120)
        time.sleep(3)                                    # drain in-flight messages
        meta = {}
        if proc:
            ct = proc.cpu_times()
            meta["broker_cpu_pct"] = round(100 * (ct.user + ct.system - cpu0[0]) / (time.time() - cpu0[1]), 1)
            meta["broker_mem_mb"] = round(proc.memory_info().rss / 1e6, 1)
        stopf.write_text("stop")
        for p in procs:
            try:
                p.wait(timeout=30)
            except subprocess.TimeoutExpired:
                p.terminate()
        (out / "meta.json").write_text(json.dumps({**meta, **spec, "mode": mode,
                                                   "scenario": scenario, "events": events}))
    finally:
        for p in procs + bg:
            if p.poll() is None:
                p.terminate()
        if broker:
            broker.terminate()
        net.stop()
        stop(ctrl)
        subprocess.run(["mn", "-c"], capture_output=True)
    res = analyze(str(out), warmup=args.warmup)
    return {**spec, "mode": mode, "scenario": scenario, **res}


FIELDS = ["suite", "name", "mode", "scenario", "pubs", "subs", "topics", "rate", "mean_ms", "median_ms",
          "p95_ms", "p99_ms", "max_ms", "throughput_msgs_per_s", "delivery_ratio_pct",
          "broker_routing_mean_ms", "cpu_pct", "mem_mb", "x"]


def append_row(path, row):
    new = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow(row)


def suite_compare(args, ryu_cmd):
    base = dict(pubs=6, subs=6, topics=3, rate=args.rate, duration=args.duration)
    for scenario in args.scenarios:
        for mode in ("static", "dynamic"):
            name = f"{mode}_{scenario}"
            print(f"\n=== compare: {name} ===", flush=True)
            r = run_one(ROOT / "results/compare" / name, mode, dict(base), scenario, ryu_cmd, args)
            r.update(suite="compare", name=name)
            append_row(ROOT / "results/summary_compare.csv", r)
            print(f"    mean={r['mean_ms']} ms  p95={r['p95_ms']} ms  delivery={r['delivery_ratio_pct']} %")


def suite_scale(args, ryu_cmd):
    q = args.quick
    plan = {
        "E1_publishers": [("pubs", v) for v in ([1, 5, 10] if q else [1, 5, 10, 25, 50])],
        "E2_subscribers": [("subs", v) for v in ([1, 5, 10] if q else [1, 5, 10, 25, 50])],
        "E3_connections": [("conns", v) for v in ([10, 50, 100] if q else [10, 50, 100, 250, 500])],
        "E4_topics": [("topics", v) for v in ([3, 10] if q else [3, 10, 25, 50])],
        "E5_msg_rate": [("rate", v) for v in ([10, 100, 500] if q else [10, 100, 500, 1000])],
    }
    for exp, steps in plan.items():
        for key, v in steps:
            spec = dict(pubs=10, subs=6, topics=3, rate=100, duration=args.scale_duration)
            if key == "conns":
                spec.update(pubs=v // 2, subs=v - v // 2)
            elif key == "topics":
                spec.update(pubs=max(10, v), subs=max(6, v), topics=v)
            else:
                spec[key] = v
            spec["pubs"] = max(spec["pubs"], 2)
            name = f"{exp}_{key}{v}"
            print(f"\n=== scale: {name} {spec} ===", flush=True)
            try:
                r = run_one(ROOT / "results/scale" / name, "dynamic", spec, "normal", ryu_cmd, args)
            except Exception as exc:                         # keep going; record the failure
                print("    FAILED:", exc)
                continue
            r.update(suite=exp, name=name, rate=spec["rate"])
            r["x"] = v
            append_row(ROOT / "results/summary_scale.csv", r)
            print(f"    mean={r['mean_ms']} ms  p95={r['p95_ms']} ms  delivery={r['delivery_ratio_pct']} %  cpu={r['cpu_pct']}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("suite", choices=["compare", "scale", "all"])
    ap.add_argument("--ryu-cmd", default="ryu-manager", help="path to ryu-manager (e.g. ~/ryu-venv/bin/ryu-manager)")
    ap.add_argument("--duration", type=float, default=40, help="publish time per compare run (s)")
    ap.add_argument("--scale-duration", type=float, default=15)
    ap.add_argument("--rate", type=float, default=100, help="total msgs/s for compare runs")
    ap.add_argument("--core-bw", type=float, default=10, help="core link Mbps")
    ap.add_argument("--event-at", type=float, default=10, help="seconds after start: congestion/failure begins")
    ap.add_argument("--event-len", type=float, default=20, help="congestion / outage length (s)")
    ap.add_argument("--warmup", type=float, default=2.0)
    ap.add_argument("--scenarios", nargs="+", default=["normal", "congestion", "failure"])
    ap.add_argument("--quick", action="store_true", help="fewer scalability steps")
    args = ap.parse_args()
    if os.geteuid() != 0:
        raise SystemExit("run with sudo")
    setLogLevel("warning")
    subprocess.run(["mn", "-c"], capture_output=True)
    (ROOT / "results").mkdir(exist_ok=True)
    if args.suite in ("compare", "all"):
        suite_compare(args, args.ryu_cmd)
    if args.suite in ("scale", "all"):
        suite_scale(args, args.ryu_cmd)
    print("\nDone. Next: python3 analysis/plot_results.py")


if __name__ == "__main__":
    main()
