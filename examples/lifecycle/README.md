# Lifecycle Examples

Application lifecycle management for KubeMQ FastStream -- CLI runner, lifespan hooks for resource setup/teardown, and async context manager usage.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [standalone_cli.py](standalone_cli.py) | Standalone CLI application using `faststream run` |
| [lifespan_management.py](lifespan_management.py) | Lifespan hooks: `on_startup`, `after_startup`, `on_shutdown` |
| [context_manager.py](context_manager.py) | Broker as an async context manager for scripts and one-off tasks |

## See Also

- [integrations/](../integrations/) -- Web framework lifecycle integration
- [quickstart/](../quickstart/) -- Getting started with `asyncio.run(app.run())`
