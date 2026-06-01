"""Prometheus metrics middleware for KubeMQ messaging.

Integrates FastStream's built-in PrometheusMiddleware to expose
message processing metrics (counts, latencies, errors) on an HTTP
endpoint that Prometheus can scrape.

Usage:
    python examples/middleware/prometheus_metrics.py

Requires:
    KubeMQ broker on localhost:50000
    pip install prometheus-client
"""

from __future__ import annotations

import asyncio
import logging

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

try:
    from faststream.prometheus import PrometheusMiddleware
    from prometheus_client import CollectorRegistry, start_http_server
except ImportError as _err:
    raise SystemExit(
        "This example requires the 'prometheus-client' package.\n"
        "Install it with:  pip install prometheus-client"
    ) from _err

registry = CollectorRegistry()
prometheus_middleware = PrometheusMiddleware(registry=registry)

broker = KubeMQBroker(
    "kubemq://localhost:50000",
    middlewares=(prometheus_middleware,),
)
app = FastStream(broker)

CHANNEL = "example.middleware.prometheus"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process messages — metrics are recorded automatically."""
    print(f"[Prometheus] Processed: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Start metrics server and publish sample messages."""
    start_http_server(port=9090, registry=registry)
    print("Prometheus metrics available at http://localhost:9090/metrics")

    for i in range(5):
        await broker.publish({"event_id": i, "type": "metric-demo"}, events=CHANNEL)

    await asyncio.sleep(2)
    print("Published 5 messages — check /metrics for counters")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
