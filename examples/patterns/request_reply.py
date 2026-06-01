"""Request-Reply Pattern -- commands and queries side-by-side.

Demonstrates the request-reply pattern using both commands (execute
an action, receive confirmation) and queries (ask a question, receive
data) in the same application.  Commands are for actions; queries
are for data retrieval.

Usage:
    python examples/patterns/request_reply.py

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream

Expected output:
    [command handler] Executing: create_user
    [caller] Command response: {'status': 'executed', 'action': 'create_user', 'success': True}
    [query handler] Looking up user_id=42
    [caller] Query response: {'user_id': 42, 'name': 'Alice', 'email': 'alice@example.com'}
    Demo complete
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

COMMAND_CHANNEL = "example.patterns.reqreply.commands"
QUERY_CHANNEL = "example.patterns.reqreply.queries"


# --- Command handler: execute an action and confirm ---


@broker.subscriber(commands=COMMAND_CHANNEL)
async def handle_command(msg: dict) -> dict:
    """Execute the command and return confirmation."""
    action = msg.get("action", "unknown")
    print(f"[command handler] Executing: {action}")
    return {"status": "executed", "action": action, "success": True}


# --- Query handler: look up data and return it ---


@broker.subscriber(queries=QUERY_CHANNEL)
async def handle_query(msg: dict) -> dict:
    """Process the query and return data."""
    user_id = msg.get("user_id")
    print(f"[query handler] Looking up user_id={user_id}")
    return {
        "user_id": user_id,
        "name": "Alice",
        "email": "alice@example.com",
    }


@app.after_startup
async def run_demo() -> None:
    """Send a command and a query, printing the responses."""
    # Command request-reply
    try:
        cmd_response = await broker.request(
            {"action": "create_user", "name": "Alice"},
            commands=COMMAND_CHANNEL,
        )
        print(f"[caller] Command response: {cmd_response}")
    except Exception as exc:
        print(f"[caller] Command failed: {exc}")

    # Query request-reply
    try:
        query_response = await broker.request(
            {"user_id": 42},
            queries=QUERY_CHANNEL,
        )
        print(f"[caller] Query response: {query_response}")
    except Exception as exc:
        print(f"[caller] Query failed: {exc}")

    await asyncio.sleep(2)
    print("Demo complete")
    await app.stop()


if __name__ == "__main__":
    asyncio.run(app.run())
