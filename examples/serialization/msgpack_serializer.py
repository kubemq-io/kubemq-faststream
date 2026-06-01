"""Msgpack serialization for compact binary messages.

Uses a custom FastStream decoder/parser to serialize messages with
msgpack instead of the default JSON codec.  Msgpack produces smaller
payloads and is faster to encode/decode for large volumes.

Usage:
    python examples/serialization/msgpack_serializer.py

Requires:
    KubeMQ broker on localhost:50000
    pip install msgpack
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

try:
    import msgpack
except ImportError as _err:
    raise SystemExit(
        "This example requires the 'msgpack' package.\nInstall with:  pip install msgpack"
    ) from _err


async def msgpack_decoder(msg: Any) -> Any:
    """Decode msgpack bytes into a Python object."""
    raw = msg.body if hasattr(msg, "body") else msg
    if isinstance(raw, bytes):
        return msgpack.unpackb(raw, raw=False)
    return raw


broker = KubeMQBroker(
    "kubemq://localhost:50000",
    decoder=msgpack_decoder,
)
app = FastStream(broker)

CHANNEL = "example.serial.msgpack"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Receive and print a msgpack-decoded message."""
    print(f"[Msgpack] Decoded: {msg} (type={type(msg).__name__})")


@app.after_startup
async def run_demo() -> None:
    """Publish msgpack-encoded messages."""
    data = {"sensor": "temp-1", "value": 23.5, "unit": "celsius"}
    packed = msgpack.packb(data, use_bin_type=True)

    await broker.publish(packed, events=CHANNEL)
    print(f"Published msgpack ({len(packed)} bytes vs ~{len(str(data))} chars JSON)")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
