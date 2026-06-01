"""FastAPI WebSocket Bridge -- KubeMQ FastStream Integrations.

Bidirectional bridge between WebSocket clients and KubeMQ events.
WebSocket messages are published to KubeMQ, and KubeMQ events are
forwarded to all connected WebSocket clients.

Usage:
    python examples/integrations/fastapi_websocket_bridge.py

    Then connect via WebSocket:
        websocat ws://localhost:8000/ws

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream fastapi uvicorn websockets

Expected output:
    WebSocket client connected
    [ws->kubemq] Published: {'from': 'websocket', ...}
    [kubemq->ws] Forwarded to 1 client(s)
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

try:
    import uvicorn
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect
except ImportError as _err:
    raise SystemExit(
        "This example requires 'fastapi', 'uvicorn', and 'websockets'.\n"
        "Install with:  pip install fastapi uvicorn websockets"
    ) from _err

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.websocket"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)

# Connected WebSocket clients
connected_clients: list[WebSocket] = []


@broker.subscriber(events=CHANNEL)
async def handle_kubemq_event(msg: dict) -> None:
    """Forward KubeMQ events to all connected WebSocket clients."""
    disconnected: list[WebSocket] = []
    for ws in connected_clients:
        try:
            await ws.send_json(msg)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        connected_clients.remove(ws)
    if connected_clients:
        print(f"[kubemq->ws] Forwarded to {len(connected_clients)} client(s)")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Start FastStream broker alongside FastAPI."""
    await faststream_app.start()
    yield
    await faststream_app.stop()


web_app = FastAPI(lifespan=lifespan, title="WebSocket-KubeMQ Bridge")


@web_app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    """Handle WebSocket connections -- bridge messages to KubeMQ."""
    await ws.accept()
    connected_clients.append(ws)
    print("WebSocket client connected")

    try:
        while True:
            data = await ws.receive_json()
            await broker.publish(data, events=CHANNEL)
            print(f"[ws->kubemq] Published: {data}")
    except WebSocketDisconnect:
        connected_clients.remove(ws)
        print("WebSocket client disconnected")


@web_app.get("/health")
async def health() -> dict:
    return {
        "broker": "connected" if await broker.ping(timeout=2.0) else "disconnected",
        "websocket_clients": len(connected_clients),
    }


if __name__ == "__main__":
    print("Starting WebSocket-KubeMQ bridge on http://localhost:8000")
    print("Connect via WebSocket at ws://localhost:8000/ws")
    uvicorn.run(web_app, host="0.0.0.0", port=8000)
