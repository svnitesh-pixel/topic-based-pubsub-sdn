#!/usr/bin/env python3
"""Background UDP traffic used to congest a core path (SDN experiments).
  bgload.py sink --port 5201
  bgload.py flood --dst 10.0.0.4 --port 5201 --mbps 9.5 --duration 20
"""
import argparse
import socket
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("role", choices=["sink", "flood"])
    ap.add_argument("--dst", default="10.0.0.4")
    ap.add_argument("--port", type=int, default=5201)
    ap.add_argument("--mbps", type=float, default=9.5)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--size", type=int, default=1000)
    a = ap.parse_args()
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if a.role == "sink":
        s.bind(("0.0.0.0", a.port))
        while True:
            s.recv(65535)
    payload = b"x" * a.size
    gap = a.size * 8 / (a.mbps * 1e6)
    end = time.time() + a.duration
    nxt = time.time()
    while time.time() < end:
        s.sendto(payload, (a.dst, a.port))
        nxt += gap
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)


if __name__ == "__main__":
    main()
