# Publish–Subscribe Messaging over TCP with SDN-managed Flows
**SDN-Socket Programming – Jackfruit Mini Project** · Python · TCP sockets · Mininet · Open vSwitch · Ryu (OpenFlow 1.3)

Publishers send topic messages to a **broker**; the broker forwards them to **subscribers** by topic over reliable **TCP**; a **Ryu controller** classifies topic traffic into flows, monitors the network and **dynamically re-routes** traffic around congestion and link failures.

## 1. Project layout
```
src/pubsub_common.py   framing (JSON + '\n'), topic->port map, socket helpers
src/broker.py          asyncio TCP broker, topic table, per-subscriber send queues      (R1-R4)
src/publisher.py       blocking-socket publisher (connects to the port of its topic)    (R1, R5)
src/subscriber.py      blocking-socket subscriber, --interactive sub/unsub, CSV log     (R1, R2, R7)
src/loadgen.py         hundreds of pubs/subs in one process (scalability tests)         (R7, R8)
src/bgload.py          UDP flood used to congest a path
sdn/pubsub_policy.py   pure-Python topology tables + reroute/failover decision logic   (R5, R6)
sdn/ryu_pubsub_controller.py   Ryu app: flows, ARP proxy, stats polling, reroute      (R5, R6)
mn/topology.py         Mininet topology (6 hosts, 5 switches, 2 core paths)
mn/run_demo.py         automated functional demo with PASS/FAIL checks
mn/run_experiments.py  static-vs-dynamic comparison + scalability sweeps E1-E5
analysis/analyze.py    mean/median/p95/p99, throughput, delivery ratio, time series      (R7)
analysis/plot_results.py  PNG figures for the report
tests/                 unit/functional tests (no Mininet needed)
docs/TEST_CASES.md     test-case table
```

## 2. Architecture
```
                 +------------------- Ryu controller (OpenFlow 1.3, TCP 6653) -------------------+
                 |  flow rules . ARP proxy . port/flow statistics . reroute . failover           |
                 +-------+-----------+-----------+-----------+-----------+------------------------+
   h1 pub ---+           |           |           |           |
             s1 ======= s2 ========= s4 --- h3 BROKER         |        Path A (default): s1-s2-s4
   h2 pub ---+  \                   /  \                       |        Path B (alternate): s1-s3-s4
                 \==== s3 =========/    s5 --- h4,h5,h6 SUBSCRIBERS
```
* Hosts: `h1,h2` publishers (10.0.0.1/2) · `h3` broker (10.0.0.3) · `h4,h5,h6` subscribers (10.0.0.4-6).
* Core links 10 Mbps / 2 ms, access links 100 Mbps. The controller never carries application traffic.
* **Topic → network class (R5):** the broker listens on 5000 (generic/subscribers), **5001 sports, 5002 news, 5003 technology**. A publisher connects to the port of its topic, so switches can classify by `tcp_dst` – OpenFlow cannot read JSON. The broker still routes by the JSON `topic` field.
* **Initial assignment:** sports→A, news→A, technology→B (slide 8). Unclassified traffic (ping, background load) → A.

### Application protocol (newline-delimited JSON)
| Message | Direction | Example |
|---|---|---|
| HELLO | client→broker | `{"type":"HELLO","id":"sub-1","role":"subscriber"}` |
| SUBSCRIBE / UNSUBSCRIBE | sub→broker | `{"type":"SUBSCRIBE","topics":["sports","news"]}` → `ACK` with current topic set |
| PUBLISH | pub→broker | `{"type":"PUBLISH","topic":"sports","msg_id":"pub-1-7","ts":t1,"payload":"..."}` |
| DELIVER | broker→sub | PUBLISH fields + `t2` (broker rx) + `t3` (broker tx) |
| ERROR | broker→client | bad JSON, unknown type, missing topic – connection survives |

Design points: `TCP_NODELAY`; one coroutine per connection; per-subscriber queue so a slow subscriber cannot block publishers (drops counted after 10 000 queued); cleanup of the topic table on any disconnect.

### SDN behaviour (controller)
1. **Proactive forwarding** – per-host IP rules on every switch; **ARP proxy** (needed because the s1-s2-s4-s3 loop would storm on flooded ARP).
2. **Classification** – priority-20 rules on s1 (`ipv4_dst=broker, tcp_dst=500x`) and s4 (return path, `tcp_src=500x`); cookie = port so per-class byte counters can be read.
3. **Monitoring** – every 1 s: port stats on s1/s4 core ports (path utilisation = max tx rate / capacity) and flow stats (per-class rate).
4. **Congestion rerouting** – if util(path) > 70 % and the busiest class (> 1 kbit/s) fits better on the other path, a flow-mod moves that class; 8 s cooldown prevents flapping; the class returns home when its home path is below 21 % projected load.
5. **Failover** – OpenFlow port-status events mark core ports down; classes **and** the default route leave the dead path immediately.
6. `PUBSUB_MODE=static` keeps the same initial rules but **never changes them** – this is the baseline for the performance comparison.

## 3. Prerequisites (Ubuntu 20.04 / 22.04 VM, as in the Mininet manual)
```bash
sudo apt update && sudo apt install -y mininet openvswitch-switch python3-pip python3-venv git \
                                       python3-psutil python3-matplotlib iperf
sudo systemctl enable --now openvswitch-switch
# Ryu in its own venv (Python 3.8-3.10; Ryu does NOT work on Python 3.12)
python3 -m venv ~/ryu-venv
~/ryu-venv/bin/pip install "setuptools<68" wheel
~/ryu-venv/bin/pip install eventlet==0.30.2 ryu
~/ryu-venv/bin/ryu-manager --version          # must print a version
sudo mn --test pingall                         # Mininet sanity check (0% dropped)
```
Clone/copy this project, then `cd pubsub-sdn` for every command below.

## 4. How to generate the output

### Step 0 – tests without Mininet (30 s)
```bash
python3 -m unittest tests.test_local tests.test_policy -v      # 14 tests, all OK
```

### Step 1 – application only, on localhost (3 terminals) → screenshot for slide 11
```bash
python3 src/broker.py                                                       # T1
python3 src/subscriber.py --id sub-1 --topics sports news                   # T2
python3 src/publisher.py --id pub-1 --topic sports --message "India won!"   # T3
python3 src/publisher.py --id pub-2 --topic technology --message "New release"
```
Broker prints connect / SUBSCRIBE / PUBLISH / matching subscribers / routing latency; subscriber prints the message and `latency = Rx - Tx`; the technology message is dropped (no subscriber).

### Step 2 – manual demo inside Mininet + SDN
```bash
# Terminal A – controller (leave running)
source ~/ryu-venv/bin/activate && ryu-manager sdn/ryu_pubsub_controller.py
# Terminal B – network
sudo mn -c && sudo python3 mn/topology.py
mininet> pingall                              # 6 hosts; 0% dropped (ARP proxy + proactive flows)
mininet> xterm h3 h4 h5 h1
h3# python3 src/broker.py
h4# python3 src/subscriber.py --broker 10.0.0.3 --id sub-1 --topics sports news --interactive
h1# python3 src/publisher.py --broker 10.0.0.3 --id pub-1 --topic sports --message hi --count 5
h4#  (type)  unsub sports        then  sub technology      <- dynamic subscriptions
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1     # tcp_dst=5001 rule has n_packets>0 (R5)
# dynamic behaviour by hand:
mininet> h4 python3 src/bgload.py sink &
mininet> h2 python3 src/bgload.py flood --dst 10.0.0.4 --mbps 9.5 --duration 30 &
          (watch Terminal A: "EVENT reroute ... reason=congestion"; re-run dump-flows s1 – output port changed 3 -> 4)
mininet> link s2 s4 down        (watch "EVENT reroute ... reason=link_failure")
mininet> link s2 s4 up
```

### Step 3 – automated functional demo (prints PASS/FAIL)
```bash
# Terminal A: controller as in Step 2   |   Terminal B:
sudo python3 mn/run_demo.py        # results/demo/* , flows_s1.txt , expected: all checks PASS
```

### Step 4 – performance evaluation (the runner starts/stops Ryu itself; stop any manual controller first)
```bash
sudo python3 mn/run_experiments.py compare --ryu-cmd ~/ryu-venv/bin/ryu-manager   # ~8 min
sudo python3 mn/run_experiments.py scale   --ryu-cmd ~/ryu-venv/bin/ryu-manager --quick   # smoke run
sudo python3 mn/run_experiments.py scale   --ryu-cmd ~/ryu-venv/bin/ryu-manager           # full E1-E5, ~30 min
```
* **compare**: normal / congestion / failure × static / dynamic → `results/summary_compare.csv`, `results/compare/<mode>_<scenario>/` (raw CSVs, `timeseries.csv`, `controller_events.jsonl`, `controller_metrics.csv`).
* **scale**: E1 publishers, E2 subscribers, E3 connections, E4 topics, E5 msg/s (steps from slide 13) → `results/summary_scale.csv`. Steps that cannot run on your VM are skipped and reported; shrink with `--quick`.

### Step 5 – figures and report tables
```bash
python3 analysis/plot_results.py     # results/figures/: compare_bars.png, timeseries_congestion.png,
                                     #   timeseries_failure.png, scale_E1..E5.png
python3 analysis/analyze.py results/compare/dynamic_failure     # prints metrics of any single run
```
Paste `summary_scale.csv` values into the slide-13 table and replace the illustrative output on slide 11 with a real screenshot.

## 5. What to expect (qualitative – numbers depend on your VM)
| Scenario | static baseline | dynamic SDN |
|---|---|---|
| normal | low latency, ~100 % delivery | same (no unnecessary moves) |
| congestion (9.5 Mbps UDP on path A) | sports/news latency grows by queueing delay (up to ~100 packets × 10 Mbps) | controller moves busiest classes to B within a few seconds; latency drops back |
| failure (s2–s4 down 20 s) | sports/news stall until link returns + TCP back-off | classes switch to B right after port-status event; short dip only |

## 6. Rubric / requirement mapping
| Rubric item | Where |
|---|---|
| TCP socket implementation | `src/*.py`: raw `socket`/`asyncio` streams, framing, concurrency, error handling |
| Mininet + SDN | `mn/topology.py`, `sdn/*` – flow rules visible with `ovs-ofctl dump-flows` |
| Socket–SDN integration | topic → TCP port → OpenFlow class; per-topic flow counters drive controller decisions; reroute is verified by app-level latency |
| Dynamic SDN functionality | congestion rerouting, failover, revert, ARP proxy, monitoring (`controller_events.jsonl`) |
| Challenging scenario | congestion + link failure + malformed input + client crash |
| Performance & baseline | `compare` (static vs dynamic) and `scale` E1–E5 |
| R1…R8 | R1 src/ · R2 sub/unsub · R3 asyncio · R4 topic table · R5 port classes · R6 `pubsub_policy` · R7 `analyze.py` · R8 `scale` |

## 7. Troubleshooting
* `Ryu failed to start` → check `results/.../ryu.log`; usually wrong Python (use 3.8-3.10 venv) or port 6653 busy (`sudo pkill -f ryu-manager`).
* `pingall` shows loss → controller not running / not connected yet; wait 3-5 s; run `sudo mn -c`.
* `Cannot find required executable ovs-...` → `sudo systemctl start openvswitch-switch`.
* Too many open files in scale tests → loadgen raises the limit itself; if 500 connections still fail, run `ulimit -n 65535` in the sudo shell.
* Leftover state → `sudo mn -c`.
* Port numbers are hard-wired in `topology.py` and `pubsub_policy.py`; change both together.

## 8. Viva cheat-sheet
* *Why can't a switch route by topic?* OpenFlow matches header fields only; the topic is in the JSON payload → one TCP port per topic class.
* *Why newline framing?* TCP is a byte stream; no message boundaries.
* *Why asyncio?* one coroutine per connection, no thread per client; per-subscriber queues avoid head-of-line blocking.
* *Why an ARP proxy?* the redundant paths form a loop; flooding ARP would cause a broadcast storm.
* *How is congestion detected?* port-stats tx-byte deltas / capacity; per-class rate from flow-stats counters (cookie = TCP port).
* *Why cooldown + revert?* to avoid flapping between paths.
* *Latency definition?* `t4 − t1` (same clock inside Mininet); broker routing latency `t3 − t2`.

## 9. Honest status
Tested here: all socket components, protocol, concurrency (14 unit/functional tests), load generator, analysis, plotting, controller decision logic. The Ryu app and Mininet scripts were written against the Ryu/Mininet APIs but need to be run in your Mininet VM; if a flow or port number behaves differently on your setup, `dump-flows` plus `ryu.log` pinpoint it quickly. Report only measured numbers from your own runs.
