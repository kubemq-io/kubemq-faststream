"""URL parsing and TLS/auth configuration helpers."""

from __future__ import annotations

import os
from typing import TypedDict
from urllib.parse import urlparse


class EnvConfig(TypedDict):
    """Resolved environment configuration."""

    url: str
    client_id: str | None
    auth_token: str | None
    tls_enabled: bool
    tls_cert_file: str | None
    tls_key_file: str | None
    tls_ca_file: str | None
    max_send_size: int
    max_receive_size: int
    default_cq_timeout: int


def parse_kubemq_url(url: str) -> tuple[str, bool]:
    """Parse kubemq:// URL into (host:port, tls_enabled).

    Schemes:
        kubemq://host:port      -> plain gRPC
        kubemq+tls://host:port  -> gRPC with TLS
        host:port               -> plain gRPC (passthrough)
    """
    if url.startswith("kubemq+tls://"):
        return url[len("kubemq+tls://") :], True
    elif url.startswith("kubemq://"):
        return url[len("kubemq://") :], False
    else:
        return url, False


def resolve_env_config(
    *,
    url: str,
    client_id: str | None,
    auth_token: str | None,
    tls_enabled: bool,
    tls_cert_file: str | None,
    tls_key_file: str | None,
    tls_ca_file: str | None,
    max_send_size: int = 4_194_304,
    max_receive_size: int = 4_194_304,
    default_cq_timeout: int = 30,
) -> EnvConfig:
    """Resolve configuration from environment variables.

    Env vars take precedence over constructor args when explicitly set.
    """
    tls_env = os.environ.get("KUBEMQ_TLS_ENABLED")
    resolved_tls = _parse_bool(tls_env) if tls_env is not None else tls_enabled

    env_max_send = os.environ.get("KUBEMQ_MAX_SEND_SIZE")
    env_max_recv = os.environ.get("KUBEMQ_MAX_RECEIVE_SIZE")
    env_cq_timeout = os.environ.get("KUBEMQ_DEFAULT_CQ_TIMEOUT")

    return EnvConfig(
        url=os.environ.get("KUBEMQ_ADDRESS", url),
        client_id=os.environ.get("KUBEMQ_CLIENT_ID", client_id),
        auth_token=os.environ.get("KUBEMQ_AUTH_TOKEN", auth_token),
        tls_enabled=resolved_tls,
        tls_cert_file=os.environ.get("KUBEMQ_TLS_CERT_FILE", tls_cert_file),
        tls_key_file=os.environ.get("KUBEMQ_TLS_KEY_FILE", tls_key_file),
        tls_ca_file=os.environ.get("KUBEMQ_TLS_CA_FILE", tls_ca_file),
        max_send_size=int(env_max_send) if env_max_send else max_send_size,
        max_receive_size=int(env_max_recv) if env_max_recv else max_receive_size,
        default_cq_timeout=int(env_cq_timeout) if env_cq_timeout else default_cq_timeout,
    )


SUPPORTED_SCHEMES = {"kubemq", "kubemq+tls"}


def validate_url(url: str) -> None:
    """Validate URL format. Only kubemq:// and kubemq+tls:// schemes accepted."""
    if "://" in url:
        scheme, rest = url.split("://", 1)
        if scheme not in SUPPORTED_SCHEMES:
            raise ValueError(
                f"Unsupported URL scheme '{scheme}'. "
                f"Use kubemq://host:port, kubemq+tls://host:port, or bare host:port"
            )
        _validate_host_port(rest)
    else:
        _validate_host_port(url)


def _validate_host_port(host_port: str) -> None:
    """Validate that host_port string is a valid host:port.

    Supports plain host:port and bracketed IPv6 [::1]:port.
    """
    if host_port.startswith("["):
        parsed = urlparse(f"grpc://{host_port}")
        if not parsed.hostname or not parsed.port:
            raise ValueError(f"Invalid KubeMQ URL: {host_port!r} — expected [host]:port")
        if not (1 <= parsed.port <= 65535):
            raise ValueError(f"Invalid port {parsed.port} — must be 1-65535")
    else:
        parts = host_port.rsplit(":", 1)
        if len(parts) != 2 or not parts[0] or not parts[1].isdigit():
            raise ValueError(f"Invalid KubeMQ URL: {host_port!r} — expected host:port")
        port = int(parts[1])
        if not (1 <= port <= 65535):
            raise ValueError(f"Invalid port {port} — must be 1-65535")


def _parse_bool(value: str) -> bool:
    """Parse boolean string. Accepts true/1/yes and false/0/no.

    Raises ValueError for unrecognized values to prevent silent misconfig.
    """
    lower = value.strip().lower()
    if lower in ("true", "1", "yes"):
        return True
    if lower in ("false", "0", "no", ""):
        return False
    raise ValueError(f"Invalid boolean value: {value!r} — use true/false, 1/0, or yes/no")
