"""Custom encoder/decoder for application-specific serialization.

Implements a simple delimiter-based format as a custom codec to
demonstrate how FastStream's ``decoder`` hook integrates with
KubeMQ messages.  In production, this pattern is used for legacy
protocols or domain-specific binary formats.

Usage:
    python examples/serialization/custom_serializer.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

SEPARATOR = "|"


def custom_encode(data: dict) -> bytes:
    """Encode a dict as 'key=value|key=value|...' bytes."""
    pairs = [f"{k}={v}" for k, v in data.items()]
    return SEPARATOR.join(pairs).encode()


def custom_decode(raw: bytes) -> dict:
    """Decode 'key=value|key=value|...' bytes back into a dict."""
    text = raw.decode() if isinstance(raw, bytes) else str(raw)
    result = {}
    for pair in text.split(SEPARATOR):
        if "=" in pair:
            k, v = pair.split("=", 1)
            result[k] = v
    return result


async def custom_decoder(msg: Any) -> Any:
    """FastStream-compatible decoder using our custom format."""
    raw = msg.body if hasattr(msg, "body") else msg
    if isinstance(raw, bytes):
        return custom_decode(raw)
    return raw


broker = KubeMQBroker(
    "kubemq://localhost:50000",
    decoder=custom_decoder,
)
app = FastStream(broker)

CHANNEL = "example.serial.custom"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Receive and print a custom-decoded message."""
    print(f"[Custom] Decoded: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish using the custom encoding format."""
    data = {"user": "alice", "action": "login", "ip": "192.168.1.1"}
    encoded = custom_encode(data)
    print(f"Encoded: {encoded!r}")

    await broker.publish(encoded, events=CHANNEL)

    await asyncio.sleep(2)
    print("Custom serialization round-trip complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
