"""Config validation, env resolution, empty client_id default.

Tests: U-11 through U-13.
"""

from __future__ import annotations

import socket
from unittest.mock import patch

import pytest

from kubemq_faststream.config import KubeMQBrokerConfig, KubeMQConnection


# U-11: test_empty_client_id_defaults_to_hostname
def test_empty_client_id_defaults_to_hostname():
    """Empty client_id defaults to socket.gethostname()."""
    config = KubeMQBrokerConfig(client_id="")
    assert config.client_id == socket.gethostname()


def test_explicit_client_id_preserved():
    """Explicit client_id is preserved."""
    config = KubeMQBrokerConfig(client_id="my-client")
    assert config.client_id == "my-client"


# U-12: test_invalid_max_send_size_raises
def test_invalid_max_send_size_raises():
    """max_send_size <= 0 raises ValueError."""
    with pytest.raises(ValueError, match="max_send_size must be > 0"):
        KubeMQBrokerConfig(max_send_size=0)

    with pytest.raises(ValueError, match="max_send_size must be > 0"):
        KubeMQBrokerConfig(max_send_size=-1)


def test_invalid_max_receive_size_raises():
    """max_receive_size <= 0 raises ValueError."""
    with pytest.raises(ValueError, match="max_receive_size must be > 0"):
        KubeMQBrokerConfig(max_receive_size=0)


def test_invalid_default_cq_timeout_raises():
    """default_cq_timeout <= 0 raises ValueError."""
    with pytest.raises(ValueError, match="default_cq_timeout must be > 0"):
        KubeMQBrokerConfig(default_cq_timeout=0)


# U-13: test_tls_cert_without_key_raises
def test_tls_cert_without_key_raises():
    """tls_cert_file without tls_key_file raises ValueError."""
    with pytest.raises(ValueError, match="tls_cert_file requires tls_key_file"):
        KubeMQBrokerConfig(tls_cert_file="/path/to/cert.pem")


def test_tls_key_without_cert_raises():
    """tls_key_file without tls_cert_file raises ValueError."""
    with pytest.raises(ValueError, match="tls_key_file requires tls_cert_file"):
        KubeMQBrokerConfig(tls_key_file="/path/to/key.pem")


def test_tls_cert_and_key_accepted():
    """tls_cert_file with tls_key_file is accepted."""
    config = KubeMQBrokerConfig(
        tls_cert_file="/path/to/cert.pem",
        tls_key_file="/path/to/key.pem",
    )
    assert config.tls_cert_file == "/path/to/cert.pem"
    assert config.tls_key_file == "/path/to/key.pem"


def test_env_resolution_overrides():
    """Environment variables override constructor args."""
    from kubemq_faststream.security import resolve_env_config

    with patch.dict(
        "os.environ",
        {
            "KUBEMQ_ADDRESS": "kubemq://remote:50001",
            "KUBEMQ_CLIENT_ID": "env-client",
        },
    ):
        env = resolve_env_config(
            url="kubemq://localhost:50000",
            client_id="ctor-client",
            auth_token=None,
            tls_enabled=False,
            tls_cert_file=None,
            tls_key_file=None,
            tls_ca_file=None,
        )
    assert env["url"] == "kubemq://remote:50001"
    assert env["client_id"] == "env-client"


def test_connect_stores_producer():
    """connect() sets the producer on the config."""
    from unittest.mock import MagicMock

    config = KubeMQBrokerConfig()
    assert config.producer is None

    mock_conn = MagicMock()
    mock_producer = MagicMock()
    config.connect(mock_conn, mock_producer)
    assert config.producer is mock_producer


def test_disconnect_clears_producer():
    """disconnect() clears the producer from config."""
    from unittest.mock import MagicMock

    config = KubeMQBrokerConfig()
    config.producer = MagicMock()
    config.disconnect()
    assert config.producer is None
