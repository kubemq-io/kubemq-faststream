# Connection Examples

Connection mechanics for KubeMQ FastStream -- how to configure broker addresses, client identifiers, channel prefixes, environment variables, and health checks.

## Prerequisites

- KubeMQ broker running (default: `localhost:50000`)
- `pip install kubemq-faststream`

## Files

| File | Description |
|------|-------------|
| [basic_connection.py](basic_connection.py) | Default connection to `localhost:50000` with health-check verification |
| [custom_address.py](custom_address.py) | Connect to a custom broker host and port |
| [client_id.py](client_id.py) | Custom `client_id` parameter for broker identification |
| [multiple_brokers.py](multiple_brokers.py) | Two KubeMQBroker instances connecting to different brokers |
| [broker_prefix.py](broker_prefix.py) | Broker-level channel prefix parameter |
| [env_var_config.py](env_var_config.py) | All configuration via environment variables (no constructor args) |
| [health_check.py](health_check.py) | Explicit `broker.ping()` health check with timeout |
| [auth_token.py](auth_token.py) | JWT authentication token via constructor argument |
| [tls_setup.py](tls_setup.py) | TLS connection using `kubemq+tls://` URL scheme |
| [mtls_setup.py](mtls_setup.py) | Mutual TLS with client certificate, key, and CA file |

## See Also

- [security/](../security/) -- Dedicated TLS, mTLS, and auth token examples
- [config/](../config/) -- Broker configuration options and URL formats
