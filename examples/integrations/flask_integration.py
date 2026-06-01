"""Flask Integration -- KubeMQ FastStream Integrations.

Run a Flask web app alongside a KubeMQ FastStream broker in a
background asyncio thread. Flask is synchronous, so the broker
runs in a dedicated thread with its own event loop.

Usage:
    python examples/integrations/flask_integration.py

    Then in another terminal:
        curl -X POST http://localhost:5000/publish \
             -H 'Content-Type: application/json' \
             -d '{"message": "hello from Flask"}'

Prerequisites:
    - KubeMQ broker running (default: localhost:50000)
    - pip install kubemq-faststream flask

Expected output:
    [broker-thread] Broker started
    [KubeMQ] Received: {'message': 'hello from Flask'}
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading

from faststream import FastStream

from kubemq_faststream import KubeMQBroker

logging.basicConfig(level=logging.INFO)

try:
    from flask import Flask, jsonify
    from flask import request as flask_request
except ImportError as _err:
    raise SystemExit("This example requires 'flask'.\nInstall with:  pip install flask") from _err

KUBEMQ_ADDRESS = os.environ.get("KUBEMQ_ADDRESS", "kubemq://localhost:50000")
CHANNEL = "example.integrations.flask"

broker = KubeMQBroker(KUBEMQ_ADDRESS)
faststream_app = FastStream(broker)

# Background event loop for the async broker
_loop: asyncio.AbstractEventLoop | None = None


@broker.subscriber(events=CHANNEL)
async def handle_message(msg: dict) -> None:
    print(f"[KubeMQ] Received: {msg}")


def _run_broker() -> None:
    """Run the FastStream broker in a background thread."""
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    print("[broker-thread] Broker started")
    _loop.run_until_complete(faststream_app.run())


def create_app() -> Flask:
    """Flask app factory."""
    app = Flask(__name__)

    # Start broker in background thread
    broker_thread = threading.Thread(target=_run_broker, daemon=True)
    broker_thread.start()

    @app.route("/publish", methods=["POST"])
    def publish():
        payload = flask_request.get_json(force=True)
        if _loop is not None:
            future = asyncio.run_coroutine_threadsafe(
                broker.publish(payload, events=CHANNEL),
                _loop,
            )
            try:
                future.result(timeout=5.0)
            except Exception as exc:
                return jsonify({"error": str(exc)}), 500
        return jsonify({"status": "published", "channel": CHANNEL})

    @app.route("/health")
    def health():
        return jsonify({"status": "ok"})

    return app


if __name__ == "__main__":
    flask_app = create_app()
    print("Starting Flask + KubeMQ app on http://localhost:5000")
    flask_app.run(host="0.0.0.0", port=5000)
