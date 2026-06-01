"""Protobuf serialization for strongly-typed message contracts.

Uses the ``protobuf`` package to serialize/deserialize messages in
Protocol Buffer format.  The custom decoder converts protobuf bytes
back into Python dicts for handler consumption.

This example defines a simple protobuf-like schema using a dict-based
approach (no .proto file compilation needed) to keep the example
self-contained.

Usage:
    python examples/serialization/protobuf_messages.py

Requires:
    KubeMQ broker on localhost:50000
    pip install protobuf
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

try:
    from google.protobuf import json_format, struct_pb2
except ImportError as _err:
    raise SystemExit(
        "This example requires the 'protobuf' package.\nInstall with:  pip install protobuf"
    ) from _err


def protobuf_encode(data: dict) -> bytes:
    """Encode a dict as protobuf Struct bytes."""
    struct = struct_pb2.Struct()
    struct.update(data)
    return struct.SerializeToString()


def protobuf_decode(raw: bytes) -> dict:
    """Decode protobuf Struct bytes back into a dict."""
    struct = struct_pb2.Struct()
    struct.ParseFromString(raw)
    return json_format.MessageToDict(struct)


async def protobuf_decoder(msg: Any) -> Any:
    """FastStream-compatible decoder for protobuf messages."""
    raw = msg.body if hasattr(msg, "body") else msg
    if isinstance(raw, bytes):
        return protobuf_decode(raw)
    return raw


broker = KubeMQBroker(
    "kubemq://localhost:50000",
    decoder=protobuf_decoder,
)
app = FastStream(broker)

CHANNEL = "example.serial.protobuf"


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    """Receive and print a protobuf-decoded message."""
    print(f"[Protobuf] Decoded: {msg}")


@app.after_startup
async def run_demo() -> None:
    """Publish using protobuf-encoded messages."""
    data = {"user_id": 42, "action": "purchase", "amount": 19.99}
    encoded = protobuf_encode(data)
    print(f"Protobuf encoded: {len(encoded)} bytes (vs ~{len(str(data))} chars JSON)")

    await broker.publish(encoded, events=CHANNEL)

    await asyncio.sleep(2)
    print("Protobuf serialization round-trip complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
