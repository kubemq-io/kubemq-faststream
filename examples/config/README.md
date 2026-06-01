# Config Examples

KubeMQ broker configuration options -- constructor parameters, URL formats, message size limits, timeouts, and AsyncAPI schema metadata.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [all_options.py](all_options.py) | Comprehensive reference for every `KubeMQBroker` constructor parameter |
| [broker_url_formats.py](broker_url_formats.py) | All URL formats: `kubemq://`, `kubemq+tls://`, with env var overrides |
| [message_size.py](message_size.py) | Configure max send/receive message size limits |
| [timeouts.py](timeouts.py) | Configure command/query timeout and graceful shutdown timeout |
| [asyncapi_schema.py](asyncapi_schema.py) | AsyncAPI schema configuration for subscriber and publisher metadata |

## See Also

- [connection/](../connection/) -- Connection mechanics and health checks
- [security/](../security/) -- TLS and authentication configuration
