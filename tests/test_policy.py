"""Unit tests for the SDN decision logic (no Ryu / Mininet needed).
Run: python3 -m unittest tests.test_policy -v"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sdn"))
from pubsub_policy import PathPolicy, out_port  # noqa: E402


class Forwarding(unittest.TestCase):
    def test_paths(self):
        self.assertEqual(out_port(1, "10.0.0.3", "A"), 3)     # s1 -> s2
        self.assertEqual(out_port(1, "10.0.0.3", "B"), 4)     # s1 -> s3
        self.assertEqual(out_port(2, "10.0.0.3", "A"), 2)     # s2 -> s4
        self.assertEqual(out_port(4, "10.0.0.3", "A"), 3)     # s4 -> broker
        self.assertEqual(out_port(4, "10.0.0.1", "B"), 2)     # return via s3
        self.assertEqual(out_port(4, "10.0.0.5", "A"), 4)     # s4 -> s5
        self.assertEqual(out_port(5, "10.0.0.5", "A"), 3)
        self.assertEqual(out_port(1, "10.0.0.2", "A"), 2)


class Decisions(unittest.TestCase):
    def mk(self, mode="dynamic"):
        return PathPolicy(mode=mode, capacity_bps=10e6, cooldown=5)

    def test_static_never_moves(self):
        p = self.mk("static")
        p.port_rate[(1, 3)] = 9e6
        p.class_rate[5001] = 1e5
        p.set_port_down(2, 2, True)
        self.assertEqual(p.decide(100), [])
        self.assertEqual(p.assign[5001], "A")

    def test_congestion_moves_busiest_class(self):
        p = self.mk()
        p.port_rate[(1, 3)] = 9e6
        p.class_rate.update({5001: 2e5, 5002: 5e5, 5003: 1e5})
        mv = p.decide(100)
        self.assertEqual([(m["cls"], m["to"], m["reason"]) for m in mv], [(5002, "B", "congestion")])
        self.assertEqual(p.decide(101), [])                   # cooldown

    def test_no_move_if_other_path_not_better(self):
        p = self.mk()
        p.port_rate[(1, 3)] = 8e6
        p.port_rate[(1, 4)] = 8e6
        p.class_rate[5001] = 1e5
        self.assertEqual(p.decide(100), [])

    def test_idle_classes_not_moved(self):
        p = self.mk()
        p.port_rate[(1, 3)] = 9e6                             # only background traffic
        self.assertEqual(p.decide(100), [])

    def test_link_failure_failover_and_default_path(self):
        p = self.mk()
        p.set_port_down(2, 2, True)
        p.set_port_down(4, 1, True)
        self.assertFalse(p.path_up("A"))
        self.assertEqual(p.default_path(), "B")
        mv = p.decide(10)
        self.assertEqual({m["cls"] for m in mv}, {5001, 5002})
        self.assertTrue(all(m["reason"] == "link_failure" and m["to"] == "B" for m in mv))
        self.assertEqual(set(p.assign.values()), {"B"})

    def test_revert_home_after_recovery(self):
        p = self.mk()
        p.assign[5001] = "B"
        p.class_rate[5001] = 2e5
        p.port_rate[(1, 3)] = 1e5                             # path A quiet again
        mv = p.decide(100)
        self.assertEqual([(m["cls"], m["to"], m["reason"]) for m in mv], [(5001, "A", "revert")])


if __name__ == "__main__":
    unittest.main()
