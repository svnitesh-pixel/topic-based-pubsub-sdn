# Test cases

| ID | Requirement | What is tested | How | Expected |
|----|-------------|----------------|-----|----------|
| T1 | R1, R4 | Message reaches only matching subscribers | `tests/test_local.py::test_routing_only_to_matching_subscribers` / `run_demo.py` step 3 | sub on `sports` receives, sub on `news,technology` does not |
| T2 | R2 | SUBSCRIBE / UNSUBSCRIBE on a live connection | `test_dynamic_subscribe_unsubscribe` / `run_demo.py` step 4 | delivery follows new topic set, no reconnect |
| T3 | R3 | 30 subscribers + 20 publishers x 25 msgs in parallel | `test_concurrent_clients` | every subscriber gets exactly 500 messages |
| T4 | robustness | Malformed JSON, unknown type, missing topic | `test_malformed_input_does_not_kill_broker` | ERROR reply, broker keeps serving |
| T5 | robustness | Client vanishes without UNSUBSCRIBE | `test_abrupt_disconnect_cleans_topic_table` | topic table cleaned, no crash |
| T6 | R5 | One TCP port per topic class | `test_topic_class_ports_all_serve_same_protocol` + `dump-flows s1` | `tcp_dst=5001/5003` rules show `n_packets>0` |
| T7 | R6 | Congestion-driven reroute decision | `tests/test_policy.py` + `compare congestion` | busiest class moves A->B when util(A) > 70 % |
| T8 | R6 | Link-failure failover | `tests/test_policy.py` + `compare failure` | classes + default route move to path B; delivery recovers |
| T9 | R7 | Latency logging | any loadgen run | `t4 - t1` per message in `recv_*.csv` |
| T10 | R8 | Scalability sweeps E1..E5 | `run_experiments.py scale` | table + figures in `results/` |
| T11 | baseline | Static vs dynamic controller | `run_experiments.py compare` | `results/summary_compare.csv`, `figures/compare_bars.png` |

Run T1-T6 (broker) and T7/T8 (policy logic) with no Mininet:
`python3 -m unittest tests.test_local tests.test_policy -v`
