#!/usr/bin/env python3
"""Expose per-container resource usage from the Docker API in Prometheus format.

cAdvisor cannot register containers when Docker uses the containerd image store
(the default since Docker 29), so this reads the same numbers from Docker's own
stats endpoint. Counters are exported as-is; use rate() for CPU and I/O rates.
"""

from concurrent.futures import ThreadPoolExecutor
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import socket
import urllib.parse

SOCKET = os.environ.get("DOCKER_SOCKET", "/var/run/docker.sock")
PORT = int(os.environ.get("EXPORTER_PORT", "9101"))
SERVICE_LABEL = "com.docker.compose.service"
PROJECT_LABEL = "com.docker.compose.project"

METRICS = [
    ("docker_container_cpu_usage_seconds_total", "counter", "Cumulative CPU time consumed by the container."),
    ("docker_container_memory_working_set_bytes", "gauge", "Memory in use excluding inactive page cache (as docker stats shows)."),
    ("docker_container_network_receive_bytes_total", "counter", "Bytes received on all container interfaces."),
    ("docker_container_network_transmit_bytes_total", "counter", "Bytes sent on all container interfaces."),
    ("docker_container_block_read_bytes_total", "counter", "Bytes read from block devices."),
    ("docker_container_block_write_bytes_total", "counter", "Bytes written to block devices."),
]


class DockerConnection(HTTPConnection):
    def __init__(self, path=SOCKET, timeout=10):
        super().__init__("localhost", timeout=timeout)
        self.socket_path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.socket_path)


def docker_get(path):
    conn = DockerConnection()
    try:
        conn.request("GET", path)
        response = conn.getresponse()
        body = response.read()
        if response.status != 200:
            raise RuntimeError(f"GET {path}: HTTP {response.status}")
        return json.loads(body)
    finally:
        conn.close()


def parse_stats(stats):
    """Map one Docker stats document to {metric name: value}."""
    memory = stats.get("memory_stats") or {}
    cache = memory.get("stats") or {}
    # cgroup v2 reports inactive_file, v1 total_inactive_file.
    inactive = cache.get("inactive_file", cache.get("total_inactive_file", 0))
    networks = (stats.get("networks") or {}).values()
    blkio = (stats.get("blkio_stats") or {}).get("io_service_bytes_recursive") or []
    blkio_total = {"read": 0, "write": 0}
    for entry in blkio:
        op = str(entry.get("op", "")).lower()
        if op in blkio_total:
            blkio_total[op] += entry.get("value", 0)
    return {
        "docker_container_cpu_usage_seconds_total": stats["cpu_stats"]["cpu_usage"]["total_usage"] / 1e9,
        "docker_container_memory_working_set_bytes": max(memory.get("usage", 0) - inactive, 0),
        "docker_container_network_receive_bytes_total": sum(n.get("rx_bytes", 0) for n in networks),
        "docker_container_network_transmit_bytes_total": sum(n.get("tx_bytes", 0) for n in networks),
        "docker_container_block_read_bytes_total": blkio_total["read"],
        "docker_container_block_write_bytes_total": blkio_total["write"],
    }


def escape(value):
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render(samples):
    """samples: list of ({"service": ..., "container": ...}, {metric: value})."""
    lines = []
    for name, kind, help_text in METRICS:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {kind}")
        for labels, values in samples:
            label_text = ",".join(f'{key}="{escape(value)}"' for key, value in sorted(labels.items()))
            lines.append(f"{name}{{{label_text}}} {values[name]}")
    return "\n".join(lines) + "\n"


def own_project():
    """The compose project this exporter belongs to, so other containers are ignored."""
    try:
        return docker_get(f"/containers/{socket.gethostname()}/json")["Config"]["Labels"].get(PROJECT_LABEL)
    except Exception:
        return None


def collect(project):
    filters = {"label": [f"{PROJECT_LABEL}={project}"]} if project else {}
    containers = docker_get("/containers/json?filters=" + urllib.parse.quote(json.dumps(filters)))

    def sample(container):
        stats = docker_get(f"/containers/{container['Id']}/stats?stream=false&one-shot=true")
        name = container["Names"][0].lstrip("/")
        labels = {"service": container["Labels"].get(SERVICE_LABEL, name), "container": name}
        return labels, parse_stats(stats)

    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(sample, containers))


def main():
    project = own_project()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path != "/metrics":
                self.send_error(404)
                return
            try:
                body = render(collect(project)).encode()
            except Exception as error:
                self.send_error(502, str(error))
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    main()
