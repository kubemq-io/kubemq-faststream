"""Default JSON serialization with dicts, dataclasses, and Pydantic models.

FastStream uses JSON as the default codec.  Plain dicts, Python
dataclasses, and Pydantic BaseModel instances are all serialized
and deserialized transparently.

Usage:
    python examples/serialization/json_default.py

Requires:
    KubeMQ broker on localhost:50000
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from faststream import FastStream
from pydantic import BaseModel

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

broker = KubeMQBroker("kubemq://localhost:50000")
app = FastStream(broker)

DICT_CH = "example.serial.json.dict"
DC_CH = "example.serial.json.dataclass"
PYDANTIC_CH = "example.serial.json.pydantic"


@dataclass
class Order:
    order_id: str
    amount: float


class User(BaseModel):
    name: str
    email: str


@broker.subscriber(events=DICT_CH)
async def handle_dict(msg: dict) -> None:
    """Receive a plain dict."""
    print(f"[Dict] {msg} (type={type(msg).__name__})")


@broker.subscriber(events=DC_CH)
async def handle_dataclass(msg: Order) -> None:
    """Receive a dataclass — FastStream deserializes automatically."""
    print(f"[Dataclass] order_id={msg.order_id}, amount={msg.amount}")


@broker.subscriber(events=PYDANTIC_CH)
async def handle_pydantic(msg: User) -> None:
    """Receive a Pydantic model — validated on deserialization."""
    print(f"[Pydantic] name={msg.name}, email={msg.email}")


@app.after_startup
async def run_demo() -> None:
    """Publish using different Python types — all serialize as JSON."""
    await broker.publish({"key": "value", "count": 42}, events=DICT_CH)

    await broker.publish(
        {"order_id": "ORD-1", "amount": 29.99},
        events=DC_CH,
    )

    await broker.publish(
        {"name": "Alice", "email": "alice@example.com"},
        events=PYDANTIC_CH,
    )

    await asyncio.sleep(2)
    print("All three serialization styles processed via default JSON codec")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
