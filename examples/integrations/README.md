# Integrations Examples

Integrate KubeMQ FastStream with popular Python web frameworks. Each example shows how to run a FastStream broker alongside a web application, sharing the async event loop and lifecycle.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`
- FastAPI examples: `pip install fastapi uvicorn`
- Django example: `pip install django`
- Flask example: `pip install flask`
- Starlette example: `pip install starlette uvicorn`

## Files

| File | Description |
|------|-------------|
| [fastapi_full.py](fastapi_full.py) | Full FastAPI integration with shared lifespan and HTTP-to-KubeMQ publishing |
| [fastapi_dependency_injection.py](fastapi_dependency_injection.py) | FastAPI `Depends()` pattern to inject the broker into route handlers |
| [fastapi_websocket_bridge.py](fastapi_websocket_bridge.py) | Bidirectional bridge between WebSocket clients and KubeMQ events |
| [django_integration.py](django_integration.py) | Django management command pattern running a KubeMQ broker |
| [flask_integration.py](flask_integration.py) | Flask app factory with background broker thread |
| [starlette_integration.py](starlette_integration.py) | Starlette lifespan integration with KubeMQ FastStream |

## See Also

- [lifecycle/](../lifecycle/) -- CLI runner and lifespan hooks
