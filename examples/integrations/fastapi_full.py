"""FastAPI Full Integration -- KubeMQ FastStream Integrations.

Enhanced FastAPI integration with KubeMQ FastStream. Includes
KUBEMQ_ADDRESS environment variable configuration, full error
handling, health checks, graceful shutdown, and structured
logging.

Usage:
    python examples/integrations/fastapi_full.py

    Then in another terminal:
        curl -X POST http://localhost:8000/publish \
             -H 'Content-Type: application/json' \
             -d '{"message": "hello from HTTP"}'

        curl http://localhost:8000/health

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream fastapi uvicorn

Expected output:
    Starting FastAPI + KubeMQ app on http://localhost:8000
    [KubeMQ] Received: {'message': 'hello from HTTP'}
"""

from __future__ import annotations

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
    from fastapi import FastAPI, HTTPException
except ImportError as _err:
    raise SystemExit(
        "This example requires 'fastapi' and 'uvicorn'.\nInstall with:  pip install fastapi uvicorn"
    ) from _err

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.fastapi"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)


@broker.subscriber(events=CHANNEL)
async def handle_kubemq_message(msg: dict) -> None:
    """Background subscriber processes KubeMQ messages."""
    logger.info("[KubeMQ] Received: %s", msg)
    print(f"[KubeMQ] Received: {msg}")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Start FastStream broker on startup, stop on shutdown."""
    logger.info("Starting FastStream broker at %s", KUBEMQ_ADDRESS)
    try:
        await faststream_app.start()
        logger.info("FastStream broker started successfully")
    except Exception:
        logger.exception("Failed to start FastStream broker")
        raise
    try:
        yield
    finally:
        logger.info("Stopping FastStream broker")
        await faststream_app.stop()
        logger.info("FastStream broker stopped")


web_app = FastAPI(
    lifespan=lifespan,
    title="FastAPI + KubeMQ Full Integration",
    description="Enhanced FastAPI integration with KubeMQ FastStream",
)


@web_app.post("/publish")
async def publish_endpoint(payload: dict) -> dict:
    """Publish a message to KubeMQ via HTTP POST.

    Args:
        payload: JSON body to publish.

    Returns:
        Status dict with channel info.
    """
    try:
        await broker.publish(payload, events=CHANNEL)
        logger.info("Published message to %s", CHANNEL)
        return {"status": "published", "channel": CHANNEL}
    except Exception as exc:
        logger.error("Failed to publish: %s", exc)
        raise HTTPException(status_code=500, detail=f"Publish failed: {exc}") from exc


@web_app.get("/health")
async def health() -> dict:
    """Health check endpoint -- verifies broker connectivity."""
    try:
        is_healthy = await broker.ping(timeout=2.0)
        status = "connected" if is_healthy else "disconnected"
    except Exception as exc:
        logger.error("Health check failed: %s", exc)
        status = "error"
    return {
        "broker": status,
        "address": KUBEMQ_ADDRESS,
        "channel": CHANNEL,
    }


@web_app.get("/")
async def root() -> dict:
    """Root endpoint with API info."""
    return {
        "app": "FastAPI + KubeMQ Full Integration",
        "endpoints": ["/publish (POST)", "/health (GET)"],
    }


if __name__ == "__main__":
    print("Starting FastAPI + KubeMQ app on http://localhost:8000")
    print("POST to /publish to send a KubeMQ message")
    print("GET /health to check broker connectivity")
    uvicorn.run(web_app, host="0.0.0.0", port=8000)
