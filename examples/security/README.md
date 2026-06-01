# Security Examples

TLS encryption, mutual TLS (mTLS), and JWT authentication for KubeMQ FastStream. Covers constructor-based configuration, environment variable configuration, and FastStream's `BaseSecurity` integration.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`
- TLS examples require valid certificates (paths are placeholders)

## Files

| File | Description |
|------|-------------|
| [plain_connection.py](plain_connection.py) | Explicit plain (non-TLS) connection as a baseline |
| [tls_server_cert.py](tls_server_cert.py) | TLS connection with `kubemq+tls://` URL and CA certificate |
| [mtls_mutual.py](mtls_mutual.py) | Mutual TLS with client certificate, key, and CA file |
| [auth_token_constructor.py](auth_token_constructor.py) | JWT auth token passed via constructor argument |
| [auth_token_env_var.py](auth_token_env_var.py) | JWT auth token supplied via `KUBEMQ_AUTH_TOKEN` env var |
| [tls_env_vars.py](tls_env_vars.py) | Full TLS stack configured entirely through environment variables |
| [tls_with_auth.py](tls_with_auth.py) | Combined TLS encryption with JWT authentication |
| [faststream_security.py](faststream_security.py) | FastStream `BaseSecurity` class integration with stdlib SSL |

## See Also

- [connection/](../connection/) -- Connection mechanics and addressing
- [config/](../config/) -- Broker configuration options
