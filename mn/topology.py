#!/usr/bin/env python3
"""Mininet topology for the pub-sub SDN project (slide 9).

        h1(pub)--+                 +-- s2 --+                    +--h3 (broker)
                 s1 ---------------+        +-- s4 -------------+
        h2(pub)--+                 +-- s3 --+                    +-- s5 --h4,h5,h6 (subscribers)
        Path A: s1-s2-s4 (default)   Path B: s1-s3-s4   (core links 10 Mbps, 2 ms)

Port numbers are fixed explicitly because the Ryu app programs them by number.
Interactive use (controller must already be running):
    sudo python3 mn/topology.py
"""
import argparse

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import OVSKernelSwitch, RemoteController


def build_net(ctrl_ip="127.0.0.1", ctrl_port=6653, core_bw=10, access_bw=100,
              core_delay="2ms", queue=100):
    net = Mininet(controller=None, switch=OVSKernelSwitch, link=TCLink,
                  autoSetMacs=False, build=False)
    net.addController("c0", controller=RemoteController, ip=ctrl_ip, port=ctrl_port)
    for i in range(1, 7):
        net.addHost(f"h{i}", ip=f"10.0.0.{i}/24", mac=f"00:00:00:00:00:0{i}")
    sw = {i: net.addSwitch(f"s{i}", dpid=f"{i:016x}", protocols="OpenFlow13") for i in range(1, 6)}
    h = {i: net.get(f"h{i}") for i in range(1, 7)}
    acc = dict(cls=TCLink, bw=access_bw, max_queue_size=queue)
    core = dict(cls=TCLink, bw=core_bw, delay=core_delay, max_queue_size=queue)
    # hosts (host port 0 = eth0)
    net.addLink(h[1], sw[1], port1=0, port2=1, **acc)
    net.addLink(h[2], sw[1], port1=0, port2=2, **acc)
    net.addLink(h[3], sw[4], port1=0, port2=3, **acc)
    net.addLink(h[4], sw[5], port1=0, port2=2, **acc)
    net.addLink(h[5], sw[5], port1=0, port2=3, **acc)
    net.addLink(h[6], sw[5], port1=0, port2=4, **acc)
    # core: path A = s1-s2-s4, path B = s1-s3-s4
    net.addLink(sw[1], sw[2], port1=3, port2=1, **core)
    net.addLink(sw[2], sw[4], port1=2, port2=1, **core)
    net.addLink(sw[1], sw[3], port1=4, port2=1, **core)
    net.addLink(sw[3], sw[4], port1=2, port2=2, **core)
    net.addLink(sw[4], sw[5], port1=4, port2=1, **acc)
    return net


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ctrl-ip", default="127.0.0.1")
    ap.add_argument("--ctrl-port", type=int, default=6653)
    args = ap.parse_args()
    setLogLevel("info")
    net = build_net(args.ctrl_ip, args.ctrl_port)
    net.build()
    net.start()
    print("Roles: h1,h2 publishers | h3 broker | h4,h5,h6 subscribers")
    print("Try:   h1 ping -c2 h3   |   sh ovs-ofctl -O OpenFlow13 dump-flows s1   |   link s2 s4 down")
    CLI(net)
    net.stop()


if __name__ == "__main__":
    main()
