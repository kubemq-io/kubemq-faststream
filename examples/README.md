# KubeMQ FastStream Integration -- Examples

Comprehensive examples covering all five KubeMQ messaging patterns (Events, Events Store, Queues, Commands, Queries) through the FastStream async framework -- organized across 19 categories with 119 runnable scripts.

## Prerequisites

- **Python 3.10+**
- **KubeMQ broker** running on `localhost:50000`
- **kubemq-faststream** installed (`pip install kubemq-faststream`)

## Quick Start

```bash
# 1. Start a KubeMQ broker
docker run -d --name kubemq -p 50000:50000 -p 9090:9090 kubemq/kubemq-community:latest

# 2. Install the package
pip install kubemq-faststream

# 3. Run your first example
python examples/quickstart/hello_world.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `KUBEMQ_ADDRESS` | `kubemq://localhost:50000` | Broker connection URL |
| `KUBEMQ_AUTH_TOKEN` | _(none)_ | JWT authentication token (security examples) |

Override the broker address for any example:

```bash
KUBEMQ_ADDRESS=kubemq://my-broker:50000 python examples/quickstart/hello_world.py
```

## Examples by Category

| # | Category | Files | Description |
|---|----------|------:|-------------|
| 1 | [quickstart/](quickstart/) | 3 | Minimal examples to get started fast |
| 2 | [connection/](connection/) | 10 | Connection mechanics -- addresses, client IDs, prefixes, health checks |
| 3 | [events/](events/) | 8 | Events pub/sub (fire-and-forget) |
| 4 | [events_store/](events_store/) | 5 | Persistent events with replay and start positions |
| 5 | [queues/](queues/) | 10 | Queue messaging with ack policies, DLQ, batch receive |
| 6 | [commands/](commands/) | 5 | Command RPC patterns |
| 7 | [queries/](queries/) | 6 | Query RPC with response caching |
| 8 | [batch_operations/](batch_operations/) | 6 | Batch publish and request across all patterns |
| 9 | [publisher/](publisher/) | 7 | Publisher decorator, headers, chaining, AsyncAPI metadata |
| 10 | [router/](router/) | 7 | Router composition, prefixes, multi-file apps |
| 11 | [middleware/](middleware/) | 4 | Prometheus, OpenTelemetry, custom middleware |
| 12 | [patterns/](patterns/) | 7 | Distributed messaging patterns (fan-out, scatter-gather, pipelines) |
| 13 | [advanced_patterns/](advanced_patterns/) | 10 | Production patterns (saga, circuit breaker, event sourcing, DLQ) |
| 14 | [serialization/](serialization/) | 4 | JSON, msgpack, protobuf, custom serialization |
| 15 | [error_handling/](error_handling/) | 7 | Error scenarios, retries, reconnection, graceful shutdown |
| 16 | [config/](config/) | 5 | Broker configuration, URL formats, message size, timeouts |
| 17 | [security/](security/) | 8 | TLS, mTLS, auth tokens, FastStream BaseSecurity |
| 18 | [integrations/](integrations/) | 6 | FastAPI, Django, Flask, Starlette, WebSocket bridge |
| 19 | [lifecycle/](lifecycle/) | 3 | CLI runner, lifespan hooks, context manager |

**Total: 119 files** across 19 categories.

## How to Run Any Example

```bash
python examples/<category>/<filename>.py
```

> **Note:** The multi-file router example has its own entry point:
> `python examples/router/multi_file_app/main.py`

## Links

- [kubemq-faststream README](../README.md) -- Integration documentation and API reference
- [KubeMQ Documentation](https://docs.kubemq.io/) -- KubeMQ broker docs
- [FastStream Documentation](https://faststream.airt.ai/latest/) -- FastStream framework docs
