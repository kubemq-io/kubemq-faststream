# Middleware Examples

FastStream middleware integration for observability and cross-cutting concerns. Includes Prometheus metrics, OpenTelemetry tracing, custom middleware, and middleware composition.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`
- Prometheus examples: `pip install prometheus-client`
- OpenTelemetry examples: `pip install opentelemetry-sdk opentelemetry-api`

## Files

| File | Description |
|------|-------------|
| [prometheus_metrics.py](prometheus_metrics.py) | Prometheus middleware with HTTP metrics endpoint |
| [opentelemetry_tracing.py](opentelemetry_tracing.py) | OpenTelemetry distributed tracing for KubeMQ messages |
| [custom_middleware.py](custom_middleware.py) | Custom middleware for logging, timing, and validation |
| [middleware_chain.py](middleware_chain.py) | Composing multiple middlewares together (Prometheus + OTel + custom) |

## See Also

- [error_handling/](../error_handling/) -- Error recovery and retry strategies
