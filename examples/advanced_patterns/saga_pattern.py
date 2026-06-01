"""Saga Pattern -- KubeMQ FastStream Advanced Patterns.

Three-step saga orchestrator using command channels with
compensating actions on failure. Steps: reserve inventory,
charge payment, ship order. If any step fails, previous
steps are compensated (reversed).

Usage:
    python examples/advanced_patterns/saga_pattern.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [saga] Step 1: reserve_inventory OK
    [saga] Step 2: charge_payment OK
    [saga] Step 3: ship_order OK
    [saga] Saga completed successfully for order-42
"""

from __future__ import annotations

import asyncio
import logging
import os

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")

broker = KubeMQBroker(KUBEMQ_ADDRESS)
app = FastStream(broker)

# --- Saga step handlers (command subscribers) ---


@broker.subscriber(commands="example.saga.reserve_inventory")
async def reserve_inventory(msg: dict) -> dict:
    print("[saga] Step 1: reserve_inventory OK")
    return {"status": "reserved", "order_id": msg["order_id"]}


@broker.subscriber(commands="example.saga.charge_payment")
async def charge_payment(msg: dict) -> dict:
    print("[saga] Step 2: charge_payment OK")
    return {"status": "charged", "order_id": msg["order_id"]}


@broker.subscriber(commands="example.saga.ship_order")
async def ship_order(msg: dict) -> dict:
    print("[saga] Step 3: ship_order OK")
    return {"status": "shipped", "order_id": msg["order_id"]}


# --- Compensation handlers ---


@broker.subscriber(commands="example.saga.compensate.inventory")
async def compensate_inventory(msg: dict) -> None:
    print(f"[compensate] Released inventory for order {msg['order_id']}")


@broker.subscriber(commands="example.saga.compensate.payment")
async def compensate_payment(msg: dict) -> None:
    print(f"[compensate] Refunded payment for order {msg['order_id']}")


# --- Saga orchestrator ---

SAGA_STEPS = [
    ("example.saga.reserve_inventory", "example.saga.compensate.inventory"),
    ("example.saga.charge_payment", "example.saga.compensate.payment"),
    ("example.saga.ship_order", None),  # last step has no compensation
]


async def run_saga(order_id: str) -> bool:
    """Execute saga steps in order; compensate on failure."""
    completed: list[str] = []

    for step_channel, compensate_channel in SAGA_STEPS:
        try:
            await broker.request(
                {"order_id": order_id},
                commands=step_channel,
                timeout=10,
            )
            if compensate_channel:
                completed.append(compensate_channel)
        except Exception as exc:
            print(f"[saga] Step failed ({step_channel}): {exc}")
            # Compensate in reverse order
            for comp_channel in reversed(completed):
                try:
                    await broker.request(
                        {"order_id": order_id},
                        commands=comp_channel,
                        timeout=10,
                    )
                except Exception as comp_exc:
                    print(f"[compensate] Failed: {comp_exc}")
            return False

    return True


@app.after_startup
async def run_demo() -> None:
    """Run the saga for a single order."""
    await asyncio.sleep(1)  # allow command subscribers to fully register
    order_id = "order-42"
    success = await run_saga(order_id)
    if success:
        print(f"[saga] Saga completed successfully for {order_id}")
    else:
        print(f"[saga] Saga failed and compensated for {order_id}")

    await asyncio.sleep(2)
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
