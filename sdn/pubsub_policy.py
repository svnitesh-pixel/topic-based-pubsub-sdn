"""Pure-Python SDN policy (no Ryu import) so it can be unit-tested anywhere.

Topology (see mininet/topology.py):

            +-- s2 --+
   h1,h2 -- s1       s4 -- h3 (broker)         s4 -- s5 -- h4,h5,h6 (subscribers)
            +-- s3 --+
   Path A = s1-s2-s4 (default)      Path B = s1-s3-s4 (alternate)

Topic classes are TCP destination ports (R5): 5001 sports, 5002 news, 5003 technology.
"""
import time

BROKER_IP = "10.0.0.3"
HOSTS = {  # ip -> (mac, dpid, port)
    "10.0.0.1": ("00:00:00:00:00:01", 1, 1),
    "10.0.0.2": ("00:00:00:00:00:02", 1, 2),
    "10.0.0.3": ("00:00:00:00:00:03", 4, 3),
    "10.0.0.4": ("00:00:00:00:00:04", 5, 2),
    "10.0.0.5": ("00:00:00:00:00:05", 5, 3),
    "10.0.0.6": ("00:00:00:00:00:06", 5, 4),
}
PUBLISHER_IPS = ("10.0.0.1", "10.0.0.2")

# per path: egress port on s1, egress port on s4 (return direction), core ports
PATHS = {
    "A": {"s1_out": 3, "s4_out": 1, "core_ports": [(1, 3), (2, 1), (2, 2), (4, 1)]},
    "B": {"s1_out": 4, "s4_out": 2, "core_ports": [(1, 4), (3, 1), (3, 2), (4, 2)]},
}
# ports whose tx rate is monitored, per path
MONITORED = {"A": [(1, 3), (4, 1)], "B": [(1, 4), (4, 2)]}

CLASSES = {5001: "sports", 5002: "news", 5003: "technology"}
INITIAL_ASSIGNMENT = {5001: "A", 5002: "A", 5003: "B"}      # slide 8 illustration


def other(path):
    return "B" if path == "A" else "A"


def out_port(dpid, dst_ip, path):
    """Next-hop port on switch `dpid` towards host `dst_ip`, using `path` where
    two paths exist (s1 and s4 only)."""
    _, dst_dpid, dst_port = HOSTS[dst_ip]
    if dst_dpid == dpid:
        return dst_port
    if dpid == 1:                       # everything not local goes towards s4
        return PATHS[path]["s1_out"]
    if dpid in (2, 3):                  # middle switches: towards s1 or towards s4
        return 1 if dst_dpid == 1 else 2
    if dpid == 4:
        if dst_dpid == 5:
            return 4
        return PATHS[path]["s4_out"]    # towards s1 side
    if dpid == 5:
        return 1
    raise ValueError(dpid)


class PathPolicy:
    """Holds link state + class->path assignment and decides reroutes.

    mode = "dynamic": congestion-aware rerouting + failover (the proposed approach)
    mode = "static" : initial assignment never changes (baseline)
    """

    def __init__(self, mode="dynamic", capacity_bps=10e6, threshold=0.7,
                 low_threshold=0.3, cooldown=8.0, min_class_bps=1000.0):
        self.mode = mode
        self.capacity = capacity_bps
        self.threshold = threshold
        self.low = low_threshold
        self.cooldown = cooldown
        self.min_class_bps = min_class_bps
        self.home = dict(INITIAL_ASSIGNMENT)
        self.assign = dict(INITIAL_ASSIGNMENT)
        self.down_ports = set()
        self.port_rate = {}             # (dpid, port) -> bits/s
        self.class_rate = {p: 0.0 for p in CLASSES}
        self.last_move = 0.0

    # ----------------------------------------------------------- link state
    def set_port_down(self, dpid, port, down):
        (self.down_ports.add if down else self.down_ports.discard)((dpid, port))

    def path_up(self, path):
        return not any(p in self.down_ports for p in PATHS[path]["core_ports"])

    def util(self, path):
        rates = [self.port_rate.get(k, 0.0) for k in MONITORED[path]]
        return max(rates) / self.capacity if rates else 0.0

    def default_path(self):
        """Path used by non-classified traffic (ping, iperf, ...)."""
        if self.mode == "dynamic" and not self.path_up("A") and self.path_up("B"):
            return "B"
        return "A"

    # ------------------------------------------------------------- decisions
    def decide(self, now=None):
        """Return a list of moves [{'cls','from','to','reason'}] and apply them."""
        if self.mode != "dynamic":
            return []
        now = time.time() if now is None else now
        moves = []

        def move(cls, to, reason):
            moves.append({"cls": cls, "from": self.assign[cls], "to": to, "reason": reason})
            self.assign[cls] = to
            self.last_move = now

        # 1) failover: a class on a dead path must leave it immediately
        for cls, p in list(self.assign.items()):
            if not self.path_up(p) and self.path_up(other(p)):
                move(cls, other(p), "link_failure")
        # classes whose home path is back up and healthy return home (after cooldown)
        if moves or now - self.last_move < self.cooldown:
            return moves

        # 2) congestion: move the busiest class off an overloaded path
        for src in ("A", "B"):
            dst = other(src)
            if not (self.path_up(src) and self.path_up(dst)):
                continue
            if self.util(src) <= self.threshold:
                continue
            cands = [c for c, p in self.assign.items()
                     if p == src and self.class_rate[c] > self.min_class_bps]
            if not cands:
                continue
            c = max(cands, key=lambda k: self.class_rate[k])
            est_dst = self.util(dst) + self.class_rate[c] / self.capacity
            if est_dst < self.util(src):                    # strictly an improvement
                move(c, dst, "congestion")
                return moves

        # 3) revert: back to the home path once it is healthy again
        for c, p in list(self.assign.items()):
            home = self.home[c]
            if p != home and self.path_up(home):
                est_home = self.util(home) + self.class_rate[c] / self.capacity
                if est_home < self.threshold * 0.7:
                    move(c, home, "revert")
                    return moves
        return moves
