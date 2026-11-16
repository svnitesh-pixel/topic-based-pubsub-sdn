#!/usr/bin/env python3
"""Make report figures from results/ (run after mn/run_experiments.py).
    python3 analysis/plot_results.py            -> results/figures/*.png
"""
import csv
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results"
FIG = RES / "figures"
COL = {"static": "#c0392b", "dynamic": "#1f77b4"}


def read_csv(p):
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def plot_compare():
    rows = read_csv(RES / "summary_compare.csv")
    if not rows:
        return
    scen = [s for s in ("normal", "congestion", "failure") if any(r["scenario"] == s for r in rows)]
    fig, ax = plt.subplots(1, 3, figsize=(14, 4))
    for a, (key, title) in zip(ax, (("mean_ms", "Mean latency (ms)"), ("p95_ms", "p95 latency (ms)"),
                                    ("delivery_ratio_pct", "Delivery ratio (%)"))):
        for i, mode in enumerate(("static", "dynamic")):
            vals = [next((f(r[key]) for r in rows if r["scenario"] == s and r["mode"] == mode), 0) for s in scen]
            a.bar([x + (i - 0.5) * 0.38 for x in range(len(scen))], vals, 0.38,
                  label=mode, color=COL[mode])
        a.set_xticks(range(len(scen)))
        a.set_xticklabels(scen)
        a.set_title(title)
        if key != "delivery_ratio_pct":
            a.set_yscale("log")
        a.grid(axis="y", alpha=.3)
    ax[0].legend()
    fig.suptitle("Static forwarding vs dynamic SDN path management")
    fig.tight_layout()
    fig.savefig(FIG / "compare_bars.png", dpi=140)
    plt.close(fig)

    for sc in ("congestion", "failure"):
        fig, ax = plt.subplots(2, 1, figsize=(10, 6), sharex=True)
        for mode in ("static", "dynamic"):
            d = RES / "compare" / f"{mode}_{sc}"
            ts = read_csv(d / "timeseries.csv")
            if not ts:
                continue
            x = [f(r["t_s"]) for r in ts]
            ax[0].plot(x, [f(r["mean_ms"]) for r in ts], label=mode, color=COL[mode])
            ax[1].plot(x, [100 * f(r["delivered"]) / f(r["expected"]) if f(r["expected"]) else float("nan")
                           for r in ts], label=mode, color=COL[mode])
            meta = json.load(open(d / "meta.json")) if (d / "meta.json").exists() else {}
            t0 = json.load(open(d / "summary.json")).get("t0", 0)
            ev = meta.get("events", {})
            if mode == "dynamic":
                for line in open(d / "controller_events.jsonl"):
                    e = json.loads(line)
                    if e["event"] == "reroute":
                        for a in ax:
                            a.axvline(e["t"] - t0, color="green", ls=":", alpha=.8)
            for k, v in ev.items():
                if mode == "static":
                    for a in ax:
                        a.axvline(v - t0, color="gray", ls="--", alpha=.7)
        ax[0].set_ylabel("mean latency (ms)")
        ax[0].set_yscale("log")
        ax[1].set_ylabel("delivered (%)")
        ax[1].set_xlabel("time since first publish (s); gray dashed = event, green dotted = SDN reroute")
        ax[0].legend()
        for a in ax:
            a.grid(alpha=.3)
        ax[0].set_title(f"Scenario: {sc}")
        fig.tight_layout()
        fig.savefig(FIG / f"timeseries_{sc}.png", dpi=140)
        plt.close(fig)


def plot_scale():
    rows = read_csv(RES / "summary_scale.csv")
    if not rows:
        return
    for exp in sorted({r["suite"] for r in rows}):
        rr = sorted([r for r in rows if r["suite"] == exp], key=lambda r: f(r.get("x", r.get("name", 0)))
                    if r.get("x") else 0)
        xs = [r["name"].split(exp + "_")[1].lstrip("abcdefghijklmnopqrstuvwxyz_") for r in rr]
        xs = [f(x) for x in xs]
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        rr, xs = [rr[i] for i in order], [xs[i] for i in order]
        fig, ax = plt.subplots(2, 2, figsize=(10, 7))
        ax[0][0].plot(xs, [f(r["mean_ms"]) for r in rr], "o-", label="mean")
        ax[0][0].plot(xs, [f(r["p95_ms"]) for r in rr], "s--", label="p95")
        ax[0][0].set_title("Latency (ms)")
        ax[0][0].legend()
        ax[0][1].plot(xs, [f(r["throughput_msgs_per_s"]) for r in rr], "o-")
        ax[0][1].set_title("Throughput (delivered msgs/s)")
        ax[1][0].plot(xs, [f(r["delivery_ratio_pct"]) for r in rr], "o-")
        ax[1][0].set_title("Delivery ratio (%)")
        ax[1][0].set_ylim(0, 105)
        ax[1][1].plot(xs, [f(r["cpu_pct"]) for r in rr], "o-", label="CPU %")
        ax2 = ax[1][1].twinx()
        ax2.plot(xs, [f(r["mem_mb"]) for r in rr], "s--", color="tab:red")
        ax2.set_ylabel("memory (MB)", color="tab:red")
        ax[1][1].set_title("Broker CPU (%) / memory (MB)")
        for a in ax.flat:
            a.grid(alpha=.3)
            a.set_xlabel(exp)
        fig.tight_layout()
        fig.savefig(FIG / f"scale_{exp}.png", dpi=140)
        plt.close(fig)


if __name__ == "__main__":
    FIG.mkdir(parents=True, exist_ok=True)
    plot_compare()
    plot_scale()
    print("figures written to", FIG)
