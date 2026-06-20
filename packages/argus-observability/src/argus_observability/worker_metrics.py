from __future__ import annotations

from prometheus_client import start_http_server


def start_metrics_server(port: int = 9091) -> None:
    start_http_server(port)
