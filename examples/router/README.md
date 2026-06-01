# Router Examples

`KubeMQRouter` for modular application composition. Split subscribers and publishers across routers, use prefixes for channel namespacing, and nest routers for large applications.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_router.py](basic_router.py) | Create a `KubeMQRouter`, add subscribers/publishers, include in broker |
| [include_router.py](include_router.py) | `broker.include_router(router)` with prefix propagation |
| [router_prefix.py](router_prefix.py) | `KubeMQRouter(prefix=)` for channel namespacing |
| [nested_routers.py](nested_routers.py) | Router including another router for deeply modular apps |
| [multi_file_app/main.py](multi_file_app/main.py) | Main app composing routers from separate modules |
| [multi_file_app/events_router.py](multi_file_app/events_router.py) | Events-focused router module |
| [multi_file_app/queues_router.py](multi_file_app/queues_router.py) | Queues-focused router module |

> **Note:** Run the multi-file app via `python examples/router/multi_file_app/main.py`

## See Also

- [publisher/](../publisher/) -- Publisher decorator patterns
