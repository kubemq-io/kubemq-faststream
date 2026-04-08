"""KubeMQ broker configuration."""

from __future__ import annotations

import socket
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs.broker import BrokerConfig

if TYPE_CHECKING:
    from kubemq.cq import AsyncCQClient
    from kubemq.pubsub import AsyncPubSubClient
    from kubemq.queues import AsyncQueuesClient

    from kubemq_faststream.producer import KubeMQProducer


@dataclass(kw_only=True)
class KubeMQBrokerConfig(BrokerConfig):
    """Configuration for KubeMQBroker.

    Extends FastStream BrokerConfig with KubeMQ-specific connection fields.
    """

    url: str = "kubemq://localhost:50000"
    client_id: str = ""
    auth_token: str = field(default="", repr=False)
    tls_enabled: bool = False
    tls_cert_file: str | None = None
    tls_key_file: str | None = None
    tls_ca_file: str | None = None
    max_send_size: int = 4_194_304
    max_receive_size: int = 4_194_304
    default_cq_timeout: int = 30
    keepalive_time_ms: int = 30_000
    keepalive_timeout_ms: int = 10_000
    producer: KubeMQProducer | None = field(default=None, repr=False)  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if not self.client_id:
            self.client_id = socket.gethostname()
        if self.max_send_size <= 0:
            raise ValueError("max_send_size must be > 0")
        if self.max_receive_size <= 0:
            raise ValueError("max_receive_size must be > 0")
        if self.default_cq_timeout <= 0:
            raise ValueError("default_cq_timeout must be > 0")
        if self.tls_cert_file and not self.tls_key_file:
            raise ValueError("tls_cert_file requires tls_key_file for mTLS")
        if self.tls_key_file and not self.tls_cert_file:
            raise ValueError("tls_key_file requires tls_cert_file for mTLS")

    def connect(self, connection: KubeMQConnection, producer: KubeMQProducer) -> None:
        """Store connection state after broker connects."""
        self.producer = producer

    def disconnect(self) -> None:
        """Clear connection state after broker disconnects."""
        self.producer = None


@dataclass
class KubeMQConnection:
    """Holds the 3 KubeMQ async clients."""

    pubsub: AsyncPubSubClient
    queues: AsyncQueuesClient
    cq: AsyncCQClient

    async def close(self) -> None:
        """Close all clients, collecting errors."""
        errors: list[Exception] = []
        for client in (self.pubsub, self.queues, self.cq):
            try:
                await client.close()
            except Exception as exc:
                errors.append(exc)
        if errors:
            # ExceptionGroup is builtin in Python 3.11+ (our minimum version)
            raise ExceptionGroup("Errors closing KubeMQ clients", errors)

    async def __aenter__(self) -> KubeMQConnection:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
