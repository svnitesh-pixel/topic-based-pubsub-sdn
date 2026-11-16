"""Ryu (OpenFlow 1.3) controller for the pub-sub SDN project (R5, R6).

* proactive per-host IP forwarding (hosts are known: static topology)
* ARP proxy (avoids broadcast storms on the redundant s1-s2/s3-s4 loop)
* topic classification: flow rules match TCP dst/src port 5001/5002/5003
* monitoring: polls port + flow statistics every INTERVAL seconds
* dynamic path modification: congestion-aware rerouting + link-failure failover

Environment variables
    PUBSUB_MODE       dynamic (default) | static  (baseline: rules never change)
    LINK_BW_MBPS      core-link capacity used for utilisation (default 10)
    CTRL_THRESHOLD    congestion threshold, fraction of capacity (default 0.7)
    CTRL_INTERVAL     polling period seconds (default 1.0)
    CTRL_COOLDOWN     min seconds between congestion moves (default 8)
    PUBSUB_LOGDIR     where metrics/event logs are written (default ./logs)
"""
import csv
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ryu.base import app_manager                                   # noqa: E402
from ryu.controller import ofp_event                               # noqa: E402
from ryu.controller.handler import (CONFIG_DISPATCHER, DEAD_DISPATCHER,   # noqa: E402
                                    MAIN_DISPATCHER, set_ev_cls)
from ryu.lib import hub                                            # noqa: E402
from ryu.lib.packet import arp, ethernet, ether_types, packet      # noqa: E402
from ryu.ofproto import ofproto_v1_3                               # noqa: E402

from pubsub_policy import (BROKER_IP, CLASSES, HOSTS, MONITORED, PATHS,   # noqa: E402
                           PUBLISHER_IPS, PathPolicy, out_port)

PRIO_MISS, PRIO_DEFAULT, PRIO_CLASS = 0, 10, 20
ETH_IP = 0x0800


class PubSubController(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        env = os.environ.get
        self.mode = env("PUBSUB_MODE", "dynamic")
        self.interval = float(env("CTRL_INTERVAL", "1.0"))
        self.policy = PathPolicy(
            mode=self.mode,
            capacity_bps=float(env("LINK_BW_MBPS", "10")) * 1e6,
            threshold=float(env("CTRL_THRESHOLD", "0.35")),
            cooldown=float(env("CTRL_COOLDOWN", "8")))
        self.dps = {}
        self._last_port = {}            # (dpid, port) -> (tx_bytes, time)
        self._last_class = {}           # cls -> (bytes, time)
        logdir = env("PUBSUB_LOGDIR", "logs")
        os.makedirs(logdir, exist_ok=True)
        self.events_f = open(os.path.join(logdir, "controller_events.jsonl"), "w")
        self.metrics_f = open(os.path.join(logdir, "controller_metrics.csv"), "w", newline="")
        self.metrics = csv.writer(self.metrics_f)
        self.metrics.writerow(["t", "util_A", "util_B", "sports", "news", "technology",
                               "rate_sports_bps", "rate_news_bps", "rate_tech_bps"])
        self.logger.info("PubSubController started, mode=%s", self.mode)
        hub.spawn(self._monitor)

    # ------------------------------------------------------------ utilities
    def _event(self, kind, **kw):
        rec = {"t": time.time(), "event": kind, **kw}
        self.events_f.write(json.dumps(rec) + "\n")
        self.events_f.flush()
        self.logger.info("EVENT %s", rec)

    @staticmethod
    def _add_flow(dp, prio, match, actions, cookie=0):
        p = dp.ofproto_parser
        inst = [p.OFPInstructionActions(dp.ofproto.OFPIT_APPLY_ACTIONS, actions)]
        dp.send_msg(p.OFPFlowMod(datapath=dp, priority=prio, match=match,
                                 instructions=inst, cookie=cookie))

    def _install_flows(self, dp):
        """(Re)install every rule of one switch from the current policy state."""
        p, ofp = dp.ofproto_parser, dp.ofproto
        dpid = dp.id
        dflt = self.policy.default_path()
        # default per-host IP forwarding
        for ip in HOSTS:
            port = out_port(dpid, ip, dflt)
            self._add_flow(dp, PRIO_DEFAULT,
                           p.OFPMatch(eth_type=ETH_IP, ipv4_dst=ip),
                           [p.OFPActionOutput(port)])
        # topic-class rules (only s1: publisher->broker, s4: broker->publisher)
        for tcp_port, path in self.policy.assign.items():
            if dpid == 1:
                m = p.OFPMatch(eth_type=ETH_IP, ip_proto=6, ipv4_dst=BROKER_IP, tcp_dst=tcp_port)
                self._add_flow(dp, PRIO_CLASS, m,
                               [p.OFPActionOutput(PATHS[path]["s1_out"])], cookie=tcp_port)
            elif dpid == 4:
                for pub in PUBLISHER_IPS:
                    m = p.OFPMatch(eth_type=ETH_IP, ip_proto=6, ipv4_dst=pub, tcp_src=tcp_port)
                    self._add_flow(dp, PRIO_CLASS, m,
                                   [p.OFPActionOutput(PATHS[path]["s4_out"])], cookie=tcp_port)

    def _reinstall_multipath(self):
        for dpid in (1, 4):
            if dpid in self.dps:
                self._install_flows(self.dps[dpid])

    # ---------------------------------------------------- switch life-cycle
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def _features(self, ev):
        dp = ev.msg.datapath
        ofp, p = dp.ofproto, dp.ofproto_parser
        self.dps[dp.id] = dp
        if dp.id == 1:                       # fresh start of the network: forget old class moves
            self.policy.assign = dict(self.policy.home)
        # forget stale link state of this switch; the real state arrives via port-desc below
        self.policy.down_ports = {k for k in self.policy.down_ports if k[0] != dp.id}
        dp.send_msg(p.OFPFlowMod(datapath=dp, command=ofp.OFPFC_DELETE,
                                 out_port=ofp.OFPP_ANY, out_group=ofp.OFPG_ANY,
                                 match=p.OFPMatch()))
        # table-miss -> controller (only ARP is handled)
        self._add_flow(dp, PRIO_MISS, p.OFPMatch(),
                       [p.OFPActionOutput(ofp.OFPP_CONTROLLER, ofp.OFPCML_NO_BUFFER)])
        self._install_flows(dp)
        if dp.id == 1 and 4 in self.dps:     # keep s4 return rules consistent with s1
            self._install_flows(self.dps[4])
        dp.send_msg(p.OFPPortDescStatsRequest(dp, 0))
        self._event("switch_connected", dpid=dp.id)

    @set_ev_cls(ofp_event.EventOFPPortDescStatsReply, [CONFIG_DISPATCHER, MAIN_DISPATCHER])
    def _port_desc(self, ev):
        dp = ev.msg.datapath
        ofp = dp.ofproto
        for d in ev.msg.body:
            if d.port_no >= ofp.OFPP_MAX:
                continue
            down = bool(d.state & ofp.OFPPS_LINK_DOWN) or bool(d.config & ofp.OFPPC_PORT_DOWN)
            self.policy.set_port_down(dp.id, d.port_no, down)

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def _state_change(self, ev):
        if ev.state == DEAD_DISPATCHER and ev.datapath and ev.datapath.id in self.dps:
            del self.dps[ev.datapath.id]
            self._event("switch_disconnected", dpid=ev.datapath.id)

    # --------------------------------------------------------------- ARP proxy
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in(self, ev):
        msg = ev.msg
        dp = msg.datapath
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        a = pkt.get_protocol(arp.arp)
        if not eth or not a or a.opcode != arp.ARP_REQUEST or a.dst_ip not in HOSTS:
            return                                           # drop everything else
        mac = HOSTS[a.dst_ip][0]
        reply = packet.Packet()
        reply.add_protocol(ethernet.ethernet(ethertype=ether_types.ETH_TYPE_ARP,
                                             dst=eth.src, src=mac))
        reply.add_protocol(arp.arp(opcode=arp.ARP_REPLY, src_mac=mac, src_ip=a.dst_ip,
                                   dst_mac=a.src_mac, dst_ip=a.src_ip))
        reply.serialize()
        p, ofp = dp.ofproto_parser, dp.ofproto
        dp.send_msg(p.OFPPacketOut(datapath=dp, buffer_id=ofp.OFP_NO_BUFFER,
                                   in_port=ofp.OFPP_CONTROLLER,
                                   actions=[p.OFPActionOutput(msg.match["in_port"])],
                                   data=reply.data))

    # ------------------------------------------------------------ link failure
    @set_ev_cls(ofp_event.EventOFPPortStatus, MAIN_DISPATCHER)
    def _port_status(self, ev):
        msg = ev.msg
        ofp = msg.datapath.ofproto
        d = msg.desc
        down = bool(d.state & ofp.OFPPS_LINK_DOWN) or bool(d.config & ofp.OFPPC_PORT_DOWN)
        self.policy.set_port_down(msg.datapath.id, d.port_no, down)
        self._event("port_status", dpid=msg.datapath.id, port=d.port_no,
                    state="down" if down else "up",
                    path_A_up=self.policy.path_up("A"), path_B_up=self.policy.path_up("B"))
        self._apply(self.policy.decide())
        if self.mode == "dynamic":
            self._reinstall_multipath()     # default route may have to switch path too

    # --------------------------------------------------------------- monitoring
    def _monitor(self):
        while True:
            hub.sleep(self.interval)
            for dp in list(self.dps.values()):
                p, ofp = dp.ofproto_parser, dp.ofproto
                if dp.id in (1, 4):
                    dp.send_msg(p.OFPPortStatsRequest(dp, 0, ofp.OFPP_ANY))
                if dp.id == 1:
                    dp.send_msg(p.OFPFlowStatsRequest(dp))
            hub.sleep(self.interval * 0.4)          # let replies arrive
            self._apply(self.policy.decide())
            pol = self.policy
            self.metrics.writerow([f"{time.time():.3f}", f"{pol.util('A'):.4f}",
                                   f"{pol.util('B'):.4f}",
                                   pol.assign[5001], pol.assign[5002], pol.assign[5003],
                                   f"{pol.class_rate[5001]:.0f}", f"{pol.class_rate[5002]:.0f}",
                                   f"{pol.class_rate[5003]:.0f}"])
            self.metrics_f.flush()

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def _port_stats(self, ev):
        now, dpid = time.time(), ev.msg.datapath.id
        for st in ev.msg.body:
            key = (dpid, st.port_no)
            if not any(key in v for v in MONITORED.values()):
                continue
            prev = self._last_port.get(key)
            self._last_port[key] = (st.tx_bytes, now)
            if prev and now > prev[1]:
                self.policy.port_rate[key] = max(0, st.tx_bytes - prev[0]) * 8 / (now - prev[1])

    @set_ev_cls(ofp_event.EventOFPFlowStatsReply, MAIN_DISPATCHER)
    def _flow_stats(self, ev):
        now = time.time()
        for st in ev.msg.body:
            if st.priority != PRIO_CLASS or st.cookie not in CLASSES:
                continue
            prev = self._last_class.get(st.cookie)
            self._last_class[st.cookie] = (st.byte_count, now)
            if prev and now > prev[1]:
                self.policy.class_rate[st.cookie] = (
                    max(0, st.byte_count - prev[0]) * 8 / (now - prev[1]))

    def _apply(self, moves):
        if not moves:
            return
        for m in moves:
            self._event("reroute", topic=CLASSES[m["cls"]], tcp_port=m["cls"],
                        frm=m["from"], to=m["to"], reason=m["reason"],
                        util_A=round(self.policy.util("A"), 3),
                        util_B=round(self.policy.util("B"), 3))
        self._reinstall_multipath()
