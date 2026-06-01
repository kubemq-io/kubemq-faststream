"""Starlette Lifespan Integration -- KubeMQ FastStream Integrations.

Integrates KubeMQ FastStream with a Starlette application using
the lifespan protocol. The broker starts and stops with the
Starlette app, and HTTP routes can publish messages to KubeMQ.

Usage:
    python examples/integrations/starlette_integration.py

    Then in another terminal:
        curl -X POST http://localhost:8000/publish \
             -H 'Content-Type: application/json' \
             -d '{"message": "hello from Starlette"}'

        curl http://localhost:8000/health

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream starlette uvicorn

Expected output:
    Starting Starlette + KubeMQ app on http://localhost:8000
    [KubeMQ] Received: {'message': 'hello from Starlette'}
"""

from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    import uvicorn
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route
except ImportError as _err:
    raise SystemExit(
        "This example requires 'starlette' and 'uvicorn'.\n"
        "Install with:  pip install starlette uvicorn"
    ) from _err

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.starlette"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)


@broker.subscriber(events=CHANNEL)
async def handle_kubemq_message(msg: dict) -> None:
    """Background subscriber processes KubeMQ messages."""
    logger.info("[KubeMQ] Received: %s", msg)
    print(f"[KubeMQ] Received: {msg}")


# -- Lifespan ---------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[None]:
    """Start FastStream broker on startup, stop on shutdown."""
    logger.info("Starting FastStream broker at %s", KUBEMQ_ADDRESS)
    await faststream_app.start()
    logger.info("FastStream broker started")
    try:
        yield
    finally:
        await faststream_app.stop()
        logger.info("FastStream broker stopped")


# -- Routes ------------------------------------------------------------------


async def publish_endpoint(request: Request) -> JSONResponse:
    """Publish a message to KubeMQ via HTTP POST."""
    try:
        body = await request.body()
        payload = json.loads(body)
        await broker.publish(payload, events=CHANNEL)
        logger.info("Published to %s", CHANNEL)
        return JSONResponse({"status": "published", "channel": CHANNEL})
    except Exception as exc:
        logger.error("Publish failed: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


async def health_endpoint(request: Request) -> JSONResponse:
    """Health check -- verifies broker connectivity."""
    try:
        is_healthy = await broker.ping(timeout=2.0)
        status = "connected" if is_healthy else "disconnected"
    except Exception:
        status = "error"
    return JSONResponse(
        {
            "broker": status,
            "address": KUBEMQ_ADDRESS,
        }
    )


async def root_endpoint(request: Request) -> JSONResponse:
    """Root endpoint with API info."""
    return JSONResponse(
        {
            "app": "Starlette + KubeMQ Integration",
            "endpoints": ["/publish (POST)", "/health (GET)"],
        }
    )


routes = [
    Route("/", root_endpoint),
    Route("/publish", publish_endpoint, methods=["POST"]),
    Route("/health", health_endpoint),
]

web_app = Starlette(routes=routes, lifespan=lifespan)


if __name__ == "__main__":
    print("Starting Starlette + KubeMQ app on http://localhost:8000")
    print("POST to /publish to send a KubeMQ message")
    uvicorn.run(web_app, host="0.0.0.0", port=8000)
