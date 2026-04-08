"""URL validation, auth token rejection.

Tests: U-14 through U-19.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from kubemq_faststream.broker import KubeMQBroker
from kubemq_faststream.security import parse_kubemq_url, validate_url


# U-14: test_empty_auth_token_raises
def test_empty_auth_token_raises():
    """Empty auth token (H-8) raises ValueError."""
    with pytest.raises(ValueError, match="KUBEMQ_AUTH_TOKEN is set but empty"):
        KubeMQBroker("kubemq://localhost:50000", auth_token="")


def test_empty_auth_token_from_env_raises():
    """Empty KUBEMQ_AUTH_TOKEN env var raises ValueError."""
    with patch.dict("os.environ", {"KUBEMQ_AUTH_TOKEN": "  "}):
        with pytest.raises(ValueError, match="KUBEMQ_AUTH_TOKEN is set but empty"):
            KubeMQBroker("kubemq://localhost:50000")


def test_none_auth_token_accepted():
    """None auth token (no auth) is accepted without error."""
    broker = KubeMQBroker("kubemq://localhost:50000", auth_token=None)
    assert broker is not None


def test_valid_auth_token_accepted():
    """Non-empty auth token is accepted."""
    broker = KubeMQBroker("kubemq://localhost:50000", auth_token="my-secret-token")
    assert broker is not None


# U-15: test_kubemq_scheme_accepted
def test_kubemq_scheme_accepted():
    """kubemq:// URL scheme is accepted."""
    validate_url("kubemq://localhost:50000")


# U-16: test_kubemq_tls_scheme_accepted
def test_kubemq_tls_scheme_accepted():
    """kubemq+tls:// URL scheme is accepted."""
    validate_url("kubemq+tls://localhost:50000")


# U-17: test_bare_host_port_accepted
def test_bare_host_port_accepted():
    """Bare host:port is accepted as passthrough."""
    validate_url("localhost:50000")


# U-18: test_http_scheme_rejected
def test_http_scheme_rejected():
    """http:// URL scheme is rejected."""
    with pytest.raises(ValueError, match="Unsupported URL scheme 'http'"):
        validate_url("http://localhost:50000")


# U-19: test_grpc_scheme_rejected
def test_grpc_scheme_rejected():
    """grpc:// URL scheme is rejected."""
    with pytest.raises(ValueError, match="Unsupported URL scheme 'grpc'"):
        validate_url("grpc://localhost:50000")


def test_parse_kubemq_url_plain():
    """parse_kubemq_url extracts host:port for kubemq:// scheme."""
    address, tls = parse_kubemq_url("kubemq://myhost:50000")
    assert address == "myhost:50000"
    assert tls is False


def test_parse_kubemq_url_tls():
    """parse_kubemq_url extracts host:port for kubemq+tls:// scheme."""
    address, tls = parse_kubemq_url("kubemq+tls://myhost:50000")
    assert address == "myhost:50000"
    assert tls is True


def test_parse_kubemq_url_bare():
    """parse_kubemq_url passes through bare host:port."""
    address, tls = parse_kubemq_url("myhost:50000")
    assert address == "myhost:50000"
    assert tls is False


def test_validate_url_invalid_host_port():
    """Missing port raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url("kubemq://localhost")


def test_validate_url_port_out_of_range():
    """Port > 65535 raises ValueError."""
    with pytest.raises(ValueError, match="must be 1-65535"):
        validate_url("kubemq://localhost:99999")


# ---- New coverage tests: _validate_host_port edge cases, _parse_bool ----


def test_validate_host_port_empty_host():
    """Empty host in host:port raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url("kubemq://:50000")


def test_validate_host_port_no_colon():
    """Host without port raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url("kubemq://justahostname")


def test_validate_host_port_non_digit_port():
    """Non-digit port raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url("kubemq://localhost:abc")


def test_validate_host_port_port_zero():
    """Port 0 is below range, raises ValueError."""
    with pytest.raises(ValueError, match="must be 1-65535"):
        validate_url("kubemq://localhost:0")


def test_validate_host_port_ipv6_valid():
    """Valid IPv6 bracketed address with port is accepted."""
    validate_url("kubemq://[::1]:50000")


def test_validate_host_port_ipv6_missing_port():
    """IPv6 address without port raises ValueError."""
    with pytest.raises(ValueError, match="expected"):
        validate_url("kubemq://[::1]")


def test_validate_host_port_bare_no_port():
    """Bare hostname without port raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url("myhost")


def test_validate_host_port_bare_empty_host():
    """Bare :port (empty host) raises ValueError."""
    with pytest.raises(ValueError, match="expected host:port"):
        validate_url(":50000")


def test_validate_host_port_port_65535():
    """Port 65535 is accepted (max valid)."""
    validate_url("kubemq://localhost:65535")


def test_validate_host_port_port_1():
    """Port 1 is accepted (min valid)."""
    validate_url("kubemq://localhost:1")


# ---- _parse_bool edge cases ----

from kubemq_faststream.security import _parse_bool


def test_parse_bool_true_variants():
    """_parse_bool accepts 'true', '1', 'yes' (case-insensitive)."""
    assert _parse_bool("true") is True
    assert _parse_bool("True") is True
    assert _parse_bool("TRUE") is True
    assert _parse_bool("1") is True
    assert _parse_bool("yes") is True
    assert _parse_bool("YES") is True
    assert _parse_bool("  true  ") is True


def test_parse_bool_false_variants():
    """_parse_bool accepts 'false', '0', 'no', '' (case-insensitive)."""
    assert _parse_bool("false") is False
    assert _parse_bool("False") is False
    assert _parse_bool("FALSE") is False
    assert _parse_bool("0") is False
    assert _parse_bool("no") is False
    assert _parse_bool("NO") is False
    assert _parse_bool("") is False
    assert _parse_bool("  false  ") is False


def test_parse_bool_invalid_raises():
    """_parse_bool raises ValueError for unrecognized values."""
    with pytest.raises(ValueError, match="Invalid boolean value"):
        _parse_bool("maybe")

    with pytest.raises(ValueError, match="Invalid boolean value"):
        _parse_bool("2")

    with pytest.raises(ValueError, match="Invalid boolean value"):
        _parse_bool("tru")


# ---- resolve_env_config edge cases ----

from kubemq_faststream.security import resolve_env_config


def test_resolve_env_config_defaults():
    """resolve_env_config returns constructor args when no env vars set."""
    with patch.dict("os.environ", {}, clear=True):
        # Clear all KUBEMQ_ env vars
        import os
        env_clean = {k: v for k, v in os.environ.items() if not k.startswith("KUBEMQ_")}
        with patch.dict("os.environ", env_clean, clear=True):
            config = resolve_env_config(
                url="kubemq://myhost:50000",
                client_id="test-id",
                auth_token="token",
                tls_enabled=False,
                tls_cert_file=None,
                tls_key_file=None,
                tls_ca_file=None,
            )
    assert config["url"] == "kubemq://myhost:50000"
    assert config["client_id"] == "test-id"
    assert config["auth_token"] == "token"
    assert config["tls_enabled"] is False


def test_resolve_env_config_env_overrides():
    """resolve_env_config uses env vars when set."""
    env = {
        "KUBEMQ_ADDRESS": "kubemq://envhost:9999",
        "KUBEMQ_CLIENT_ID": "env-client",
        "KUBEMQ_AUTH_TOKEN": "env-token",
        "KUBEMQ_TLS_ENABLED": "true",
        "KUBEMQ_TLS_CERT_FILE": "/cert.pem",
        "KUBEMQ_TLS_KEY_FILE": "/key.pem",
        "KUBEMQ_TLS_CA_FILE": "/ca.pem",
        "KUBEMQ_MAX_SEND_SIZE": "8388608",
        "KUBEMQ_MAX_RECEIVE_SIZE": "8388608",
        "KUBEMQ_DEFAULT_CQ_TIMEOUT": "60",
    }
    with patch.dict("os.environ", env, clear=False):
        config = resolve_env_config(
            url="kubemq://localhost:50000",
            client_id="default",
            auth_token="default",
            tls_enabled=False,
            tls_cert_file=None,
            tls_key_file=None,
            tls_ca_file=None,
        )
    assert config["url"] == "kubemq://envhost:9999"
    assert config["client_id"] == "env-client"
    assert config["auth_token"] == "env-token"
    assert config["tls_enabled"] is True
    assert config["tls_cert_file"] == "/cert.pem"
    assert config["tls_key_file"] == "/key.pem"
    assert config["tls_ca_file"] == "/ca.pem"
    assert config["max_send_size"] == 8388608
    assert config["max_receive_size"] == 8388608
    assert config["default_cq_timeout"] == 60


def test_resolve_env_config_tls_bool_from_env():
    """resolve_env_config uses _parse_bool for KUBEMQ_TLS_ENABLED."""
    with patch.dict("os.environ", {"KUBEMQ_TLS_ENABLED": "yes"}, clear=False):
        config = resolve_env_config(
            url="kubemq://localhost:50000",
            client_id=None,
            auth_token=None,
            tls_enabled=False,
            tls_cert_file=None,
            tls_key_file=None,
            tls_ca_file=None,
        )
    assert config["tls_enabled"] is True
