#!/usr/bin/env python3
"""Compute latency / throughput / delivery metrics from loadgen CSV logs (R7, R8).

A run directory holds   sent_*.csv   recv_*.csv   recv_*.subs.json   [meta.json]
    python3 analysis/analyze.py results/compare/dynamic_congestion
"""
import csv
import glob
import json
import math
import os
import sys
from collections import Counter


def pct(sorted_vals, q):
    """Linear-interpolated percentile (q in 0..100) of an already sorted list."""
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q / 100.0
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def _rows(pattern):
    for path in sorted(glob.glob(pattern)):
        with open(path, newline="") as f:
            yield from csv.DictReader(f)


def analyze(run_dir, warmup=2.0, bin_s=1.0):
    sent = [(r["msg_id"], r["topic"], float(r["t1"])) for r in _rows(os.path.join(run_dir, "sent_*.csv"))]
    recv = [(r["sub_id"], r["msg_id"], r["topic"], float(r["t1"]), float(r["t2"]),
             float(r["t3"]), float(r["t4"])) for r in _rows(os.path.join(run_dir, "recv_*.csv"))]
    subs = {}
    for p in glob.glob(os.path.join(run_dir, "recv_*.subs.json")):
        subs.update(json.load(open(p)))
    meta = {}
    mp = os.path.join(run_dir, "meta.json")
    if os.path.exists(mp):
        meta = json.load(open(mp))
    if not sent:
        raise SystemExit(f"no sent_*.csv in {run_dir}")

    per_topic_subs = Counter(t for ts in subs.values() for t in ts)
    t0 = min(s[2] for s in sent)
    window = [s for s in sent if s[2] >= t0 + warmup]
    win_ids = {s[0] for s in window}
    expected = sum(per_topic_subs.get(s[1], 0) for s in window)

    seen, lats, route, per_sec = set(), [], [], {}
    for sub, mid, topic, t1, t2, t3, t4 in recv:
        if (sub, mid) in seen:
            continue
        seen.add((sub, mid))
        lat = (t4 - t1) * 1000
        b = int((t1 - t0) // bin_s)
        per_sec.setdefault(b, []).append(lat)
        if mid in win_ids:
            lats.append(lat)
            route.append((t3 - t2) * 1000)
    lats.sort()
    span = (max(s[2] for s in window) - min(s[2] for s in window)) if len(window) > 1 else 0
    received = len(lats)
    out = {
        "sent": len(window), "expected": expected, "received": received,
        "delivery_ratio_pct": round(100.0 * received / expected, 2) if expected else float("nan"),
        "mean_ms": round(sum(lats) / received, 3) if received else float("nan"),
        "median_ms": round(pct(lats, 50), 3), "p95_ms": round(pct(lats, 95), 3),
        "p99_ms": round(pct(lats, 99), 3), "max_ms": round(lats[-1], 3) if lats else float("nan"),
        "broker_routing_mean_ms": round(sum(route) / len(route), 4) if route else float("nan"),
        "throughput_msgs_per_s": round(received / span, 2) if span else float("nan"),
        "cpu_pct": meta.get("broker_cpu_pct"), "mem_mb": meta.get("broker_mem_mb"),
    }
    # per-second series, binned by publish time (undelivered bins show 0 delivered)
    sent_bins = Counter(int((s[2] - t0) // bin_s) for s in sent)
    series = []
    for b in range(max(sent_bins) + 1):
        v = sorted(per_sec.get(b, []))
        exp_b = sum(per_topic_subs.get(s[1], 0) for s in sent if int((s[2] - t0) // bin_s) == b)
        series.append({"t_s": b * bin_s, "sent": sent_bins.get(b, 0), "expected": exp_b,
                       "delivered": len(v),
                       "mean_ms": round(sum(v) / len(v), 3) if v else "",
                       "p95_ms": round(pct(v, 95), 3) if v else ""})
    with open(os.path.join(run_dir, "timeseries.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(series[0]))
        w.writeheader()
        w.writerows(series)
    out["t0"] = t0
    with open(os.path.join(run_dir, "summary.json"), "w") as f:
        json.dump(out, f, indent=2)
    return out


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    for k, v in analyze(sys.argv[1]).items():
        print(f"{k:26s} {v}")
