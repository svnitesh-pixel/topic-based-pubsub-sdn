"""Shared helpers for the pub-sub system: framing, topic->port map, socket utils.

Wire protocol (one JSON object per line, UTF-8, terminated by '\\n'):
    HELLO        {"type":"HELLO","id":"sub-1","role":"subscriber"}
    SUBSCRIBE    {"type":"SUBSCRIBE","topic":"sports"}   (or "topics":[...])
    UNSUBSCRIBE  {"type":"UNSUBSCRIBE","topic":"sports"}
    PUBLISH      {"type":"PUBLISH","topic":"sports","msg_id":"pub-1-7","ts":<t1>,"payload":"..."}
    DELIVER      broker -> subscriber: PUBLISH fields + "t2" (broker rx) + "t3" (broker tx)
    ACK / ERROR  broker -> client control replies
TCP is a byte stream with no message boundaries, hence newline framing.
"""
import json
import resource
import socket
import time

BROKER_PORT = 5000                      # generic port (subscribers, unknown topics)
TOPIC_PORTS = {                         # one TCP port per topic class (R5)
    "sports": 5001,
    "news": 5002,
    "technology": 5003,
}
MAX_LINE = 1 << 20                      # 1 MiB hard limit for a single message


def port_for_topic(topic: str) -> int:
    """Topic class -> broker TCP port. Unknown topics use the generic port."""
    return TOPIC_PORTS.get(topic, BROKER_PORT)


def encode(msg: dict) -> bytes:
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode("utf-8")


def decode(line: bytes) -> dict:
    msg = json.loads(line.decode("utf-8"))
    if not isinstance(msg, dict):
        raise ValueError("message must be a JSON object")
    return msg


def set_nodelay(sock) -> None:
    """Disable Nagle so small messages are not delayed (matters for latency)."""
    if sock is not None:
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass


def connect(host: str, port: int, retries: int = 0, delay: float = 1.0,
            timeout: float = 5.0) -> socket.socket:
    """Blocking TCP connect with optional retries."""
    last = None
    for _ in range(retries + 1):
        try:
            s = socket.create_connection((host, port), timeout=timeout)
            s.settimeout(None)
            set_nodelay(s)
            return s
        except OSError as exc:
            last = exc
            time.sleep(delay)
    raise last


def raise_fd_limit() -> None:
    """Allow many concurrent TCP connections (scalability tests)."""
    try:
        _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    except (ValueError, OSError):
        pass
