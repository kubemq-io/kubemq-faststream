"""Composing multiple middlewares into a chain.

Stacks a custom logging middleware with optional Prometheus and
OpenTelemetry middlewares.  Each middleware wraps the handler in
order, so the first middleware in the list is the outermost wrapper.

Usage:
    python examples/middleware/middleware_chain.py

Requires:
    KubeMQ broker on localhost:50000
    Optional: pip install prometheus-client opentelemetry-sdk opentelemetry-api
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from faststream import BaseMiddleware, FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)
chain_logger = logging.getLogger("middleware_chain")


class RequestIdMiddleware(BaseMiddleware):
    """Assigns a sequential request ID to each processed message."""

    _counter: int = 0

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: Any,
    ) -> Any:
        RequestIdMiddleware._counter += 1
        req_id = RequestIdMiddleware._counter
        chain_logger.info("[ReqID=%d] Before handler", req_id)
        result = await call_next(msg)
        chain_logger.info("[ReqID=%d] After handler", req_id)
        return result


class ErrorCatchMiddleware(BaseMiddleware):
    """Catches handler exceptions and logs them without crashing."""

    async def consume_scope(
        self,
        call_next: Callable[[Any], Awaitable[Any]],
        msg: Any,
    ) -> Any:
        try:
            return await call_next(msg)
        except Exception:
            chain_logger.exception("Handler error caught by middleware")
            return None


middlewares: list[Any] = [RequestIdMiddleware, ErrorCatchMiddleware]

try:
    from faststream.prometheus import PrometheusMiddleware
    from prometheus_client import CollectorRegistry

    registry = CollectorRegistry()
    middlewares.append(PrometheusMiddleware(registry=registry))
    chain_logger.info("Prometheus middleware added to chain")
except ImportError:
    chain_logger.info("prometheus-client not installed — skipping Prometheus middleware")

try:
    from faststream.opentelemetry import TelemetryMiddleware
    from opentelemetry.sdk.trace import TracerProvider

    middlewares.append(TelemetryMiddleware(tracer_provider=TracerProvider()))
    chain_logger.info("OpenTelemetry middleware added to chain")
except ImportError:
    chain_logger.info("opentelemetry-sdk not installed — skipping OTel middleware")


broker = KubeMQBroker(
    "kubemq://localhost:50000",
    middlewares=middlewares,
)
app = FastStream(broker)

CHANNEL = "example.middleware.chain"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Process message through the full middleware chain."""
    print(f"[Chain] Handled: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish messages through the composed middleware stack."""
    for i in range(3):
        await broker.publish({"step": i}, events=CHANNEL)

    await asyncio.sleep(2)
    print(f"Processed 3 messages through {len(middlewares)}-middleware chain")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
