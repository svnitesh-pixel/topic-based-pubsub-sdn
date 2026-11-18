import os
import sys
import json
import time
import socket
import threading
import subprocess
import csv
from collections import deque
from pathlib import Path

from flask import Flask, request, jsonify, render_template_string

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from pubsub_common import encode, decode, port_for_topic, BROKER_PORT

app = Flask(__name__)

ROOT = Path(__file__).resolve().parent
EVENT_FILE = ROOT / "logs/controller_events.jsonl"
METRICS_FILE = ROOT / "logs/controller_metrics.csv"

messages = deque(maxlen=100)
message_lock = threading.Lock()
subscriber_state = {"connected": False, "error": ""}
subscriber_started = False

TOPICS = ["sports", "news", "technology"]


def subscriber_worker():
    while True:
        sock = None
        try:
            sock = socket.create_connection(("127.0.0.1", BROKER_PORT), timeout=5)
            sock.settimeout(1)

            sock.sendall(encode({
                "type": "HELLO",
                "id": "web-dashboard-subscriber",
                "role": "subscriber"
            }))

            sock.sendall(encode({
                "type": "SUBSCRIBE",
                "topics": TOPICS
            }))

            subscriber_state["connected"] = True
            subscriber_state["error"] = ""

            buffer = b""

            while True:
                try:
                    data = sock.recv(65536)
                    if not data:
                        raise ConnectionError("Broker disconnected")

                    buffer += data

                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        if not line:
                            continue

                        msg = decode(line)

                        if msg.get("type") == "DELIVER":
                            with message_lock:
                                messages.appendleft({
                                    "topic": msg.get("topic", ""),
                                    "payload": msg.get("payload", ""),
                                    "msg_id": msg.get("msg_id", ""),
                                    "time": time.strftime("%H:%M:%S"),
                                    "latency": round(
                                        (msg.get("t3", 0) - msg.get("ts", 0)) * 1000,
                                        2
                                    ) if msg.get("t3") and msg.get("ts") else None
                                })

                except socket.timeout:
                    continue

        except Exception as exc:
            subscriber_state["connected"] = False
            subscriber_state["error"] = str(exc)
            time.sleep(3)

        finally:
            subscriber_state["connected"] = False
            if sock:
                sock.close()


def start_subscriber():
    global subscriber_started
    if not subscriber_started:
        subscriber_started = True
        threading.Thread(target=subscriber_worker, daemon=True).start()


def port_open(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.5):
            return True
    except OSError:
        return False


def read_jsonl(path):
    result = []
    if path.exists():
        try:
            with path.open() as f:
                for line in f:
                    try:
                        result.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        except OSError:
            pass
    return result[-30:][::-1]


def read_metrics():
    if not METRICS_FILE.exists():
        return []

    try:
        with METRICS_FILE.open() as f:
            rows = list(csv.DictReader(f))
        return rows[-20:][::-1]
    except (OSError, csv.Error):
        return []


@app.route("/")
def home():
    start_subscriber()
    return render_template_string(PAGE)


@app.route("/api/publish", methods=["POST"])
def publish():
    data = request.get_json(silent=True) or {}
    topic = data.get("topic", "").strip()
    payload = data.get("message", "").strip()

    if topic not in TOPICS:
        return jsonify({"error": "Enter a topic"}), 400

    if not payload:
        return jsonify({"error": "Enter a message"}), 400

    port = port_for_topic(topic)

    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as sock:
            sock.sendall(encode({
                "type": "HELLO",
                "id": "web-dashboard-publisher",
                "role": "publisher"
            }))
            sock.sendall(encode({
                "type": "PUBLISH",
                "topic": topic,
                "msg_id": f"web-{int(time.time() * 1000)}",
                "ts": time.time(),
                "payload": payload
            }))

        return jsonify({
            "success": True,
            "message": f"Published to {topic} through TCP port {port}"
        })

    except OSError as exc:
        return jsonify({"error": str(exc)}), 503


@app.route("/api/messages")
def get_messages():
    with message_lock:
        data = list(messages)
    return jsonify({
        "messages": data,
        "subscriber": subscriber_state
    })


@app.route("/api/sdn")
def sdn_status():
    try:
        result = subprocess.run(
            ["pgrep", "-af", "[r]yu-manager"],
            capture_output=True, text=True, timeout=2
        )
        running = result.returncode == 0
        process = result.stdout.strip()
    except Exception:
        running = False
        process = ""

    return jsonify({
        "controller_running": running,
        "controller_process": process,
        "broker_ports": {
            str(p): port_open(p) for p in [5000, 5001, 5002, 5003]
        },
        "events": read_jsonl(EVENT_FILE),
        "metrics": read_metrics()
    })


PAGE = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pub-Sub SDN Dashboard</title>
<style>
* { box-sizing: border-box; }
body {
    margin: 0; background: #101827; color: #e5e7eb;
    font-family: Arial, sans-serif;
}
header {
    padding: 24px 5%; background: #172338;
    border-bottom: 1px solid #34445c;
}
header h1 { margin: 0 0 8px; color: #60a5fa; }
header p { margin: 0; color: #aab8cc; }
main {
    width: 90%; max-width: 1300px;
    margin: 25px auto;
}
.grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    gap: 20px;
}
.card {
    background: #1c293c; padding: 22px;
    border-radius: 12px; border: 1px solid #34445c;
}
.card h2 { margin-top: 0; color: #60a5fa; }
input, select, button {
    width: 100%; padding: 12px; margin: 9px 0;
    border-radius: 7px; border: 1px solid #52647d;
    font-size: 15px;
}
input, select { background: #101827; color: white; }
button {
    background: #2563eb; color: white;
    border: none; cursor: pointer; font-weight: bold;
}
button:hover { background: #1d4ed8; }
.status { font-weight: bold; margin: 10px 0; }
.online { color: #4ade80; }
.offline { color: #f87171; }
.message, .event {
    background: #101827; padding: 12px;
    border-radius: 7px; margin: 10px 0;
    overflow-wrap: anywhere;
}
.small { color: #9ca3af; font-size: 13px; }
.wide { margin-top: 20px; }
table { width: 100%; border-collapse: collapse; }
td, th {
    padding: 9px; border-bottom: 1px solid #34445c;
    text-align: left; font-size: 13px;
}
.table-wrap { overflow-x: auto; }
</style>
</head>
<body>

<header>
<h1>Publish–Subscribe SDN Dashboard</h1>
<p>TCP Messaging | Topic-Based Routing | SDN Monitoring</p>
</header>

<main>
<div class="grid">

<section class="card">
<h2>Publisher</h2>
<label>Choose topic</label>
<select id="topic">
<option>sports</option>
<option>news</option>
<option>technology</option>
</select>
<label>Message</label>
<input id="message" placeholder="Enter your message">
<button onclick="publishMessage()">Publish Message</button>
<p id="publishStatus" class="small">Ready to publish.</p>
</section>

<section class="card">
<h2>Subscriber</h2>
<p>Connection:
<span id="subStatus" class="status offline">Connecting...</span>
</p>
<p class="small">Subscribed topics: sports, news, technology</p>
<div id="messages">
<p class="small">Waiting for messages...</p>
</div>
</section>

<section class="card">
<h2>SDN Monitor</h2>
<p>Controller:
<span id="controllerStatus" class="status offline">Checking...</span>
</p>
<p>Broker ports:</p>
<div id="ports" class="small">Checking...</div>
<p class="small">Controller metrics and events are read from actual log files.</p>
</section>

</div>

<section class="card wide">
<h2>SDN Rerouting Events</h2>
<div id="events"><p class="small">No events loaded.</p></div>
</section>

<section class="card wide">
<h2>Network Metrics</h2>
<div class="table-wrap">
<table>
<thead>
<tr>
<th>Time</th><th>Path A</th><th>Path B</th>
<th>Sports</th><th>News</th><th>Technology</th>
</tr>
</thead>
<tbody id="metrics">
<tr><td colspan="6">Waiting for controller metrics...</td></tr>
</tbody>
</table>
</div>
</section>
</main>

<script>
async function publishMessage() {
    const topic = document.getElementById("topic").value;
    const message = document.getElementById("message").value;

    const response = await fetch("/api/publish", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({topic, message})
    });

    const data = await response.json();
    document.getElementById("publishStatus").textContent =
        data.message || data.error;

    if (data.success) document.getElementById("message").value = "";
}

async function refreshMessages() {
    const response = await fetch("/api/messages");
    const data = await response.json();

    const status = document.getElementById("subStatus");
    status.textContent = data.subscriber.connected ? "Connected" : "Disconnected";
    status.className = data.subscriber.connected ? "status online" : "status offline";

    const box = document.getElementById("messages");

    if (!data.messages.length) {
        box.innerHTML = '<p class="small">Waiting for messages...</p>';
        return;
    }

    box.innerHTML = data.messages.map(m =>
        `<div class="message">
        <b>${escapeHtml(m.topic)}</b> — ${escapeHtml(m.payload)}
        <div class="small">${escapeHtml(m.time)} | ID: ${escapeHtml(m.msg_id)}
        | Latency: ${m.latency === null ? "N/A" : m.latency + " ms"}</div>
        </div>`
    ).join("");
}

function escapeHtml(value) {
    return String(value).replace(/[&<>"']/g, c =>
        ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c])
    );
}

async function refreshSDN() {
    const response = await fetch("/api/sdn");
    const data = await response.json();

    const controller = document.getElementById("controllerStatus");
    controller.textContent = data.controller_running ? "Running" : "Not running";
    controller.className = data.controller_running ? "status online" : "status offline";

    document.getElementById("ports").innerHTML =
        Object.entries(data.broker_ports).map(([p, up]) =>
            `Port ${p}: <span class="${up ? "online" : "offline"}">${up ? "OPEN" : "CLOSED"}</span>`
        ).join("<br>");

    document.getElementById("events").innerHTML =
        data.events.length ? data.events.map(e =>
            `<div class="event">
            <b>${escapeHtml(e.event)}</b> — ${escapeHtml(JSON.stringify(e))}
            </div>`
        ).join("") : '<p class="small">No controller events recorded yet.</p>';

    document.getElementById("metrics").innerHTML =
        data.metrics.length ? data.metrics.map(m =>
            `<tr>
            <td>${escapeHtml(m.t)}</td>
            <td>${escapeHtml(m.util_A)}</td>
            <td>${escapeHtml(m.util_B)}</td>
            <td>${escapeHtml(m.sports)}</td>
            <td>${escapeHtml(m.news)}</td>
            <td>${escapeHtml(m.technology)}</td>
            </tr>`
        ).join("") : '<tr><td colspan="6">No controller metrics available yet.</td></tr>';
}

setInterval(refreshMessages, 1000);
setInterval(refreshSDN, 3000);
refreshMessages();
refreshSDN();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    print("Starting Pub-Sub SDN Dashboard...")
    print("Open http://127.0.0.1:8000 in your browser")
    app.run(host="127.0.0.1", port=8000, debug=False, threaded=True)
