"""KubeMQBroker -- main broker class with lifecycle, publish, and request."""

from __future__ import annotations

import asyncio
import logging
import re
import socket
from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

import anyio
from fast_depends import Provider, dependency_provider
from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger.state import LoggerState
from faststream.specification.schema import BrokerSpec
from typing_extensions import Self

from kubemq_faststream.config import KubeMQBrokerConfig, KubeMQConnection
from kubemq_faststream.message import KubeMQRawMessage
from kubemq_faststream.producer import KubeMQProducer
from kubemq_faststream.registrator import KubeMQRegistrator, _resolve_pattern
from kubemq_faststream.response import KubeMQPublishCommand
from kubemq_faststream.schemas import FeatureNotSupportedException, KubeMQPattern
from kubemq_faststream.security import parse_kubemq_url, resolve_env_config, validate_url

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

logger = logging.getLogger("kubemq_faststream")


class KubeMQBroker(
    KubeMQRegistrator,
    BrokerUsecase[KubeMQRawMessage, KubeMQConnection, KubeMQBrokerConfig],
):
    """KubeMQ broker adapter for FastStream.

    Creates 3 internal KubeMQ SDK clients on connect (one gRPC channel each):
    - AsyncPubSubClient (Events, EventsStore)
    - AsyncQueuesClient (Queues)
    - AsyncCQClient (Commands, Queries)

    Example::

        broker = KubeMQBroker("kubemq://localhost:50000")

        @broker.subscriber(queues="orders")
        async def handle(order: dict) -> None:
            print(order)
    """

    _connection: KubeMQConnection | None
    _connect_lock: asyncio.Lock

    def __init__(
        self,
        url: str = "kubemq://localhost:50000",
        *,
        client_id: str | None = None,
        auth_token: str | None = None,
        tls_enabled: bool = False,
        tls_cert_file: str | None = None,
        tls_key_file: str | None = None,
        tls_ca_file: str | None = None,
        max_send_size: int = 4_194_304,
        max_receive_size: int = 4_194_304,
        default_cq_timeout: int = 30,
        keepalive_time_ms: int = 30_000,
        keepalive_timeout_ms: int = 10_000,
        graceful_timeout: float | None = 15.0,
        # FastStream standard broker options (mirrors KafkaBroker pattern):
        decoder: CustomCallable | None = None,
        parser: CustomCallable | None = None,
        dependencies: Iterable[Dependant] = (),
        middlewares: Sequence[BrokerMiddleware[Any, Any]] = (),
        routers: Iterable[KubeMQRegistrator] = (),
        # AsyncAPI / specification args:
        security: BaseSecurity | None = None,
        specification_url: str | Iterable[str] | None = None,
        protocol: str | None = None,
        protocol_version: str | None = None,
        description: str | None = None,
        tags: Iterable[Tag | TagDict] = (),
        # Logging args:
        logger: LoggerProto | None = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args:
        apply_types: bool = True,
        serializer: SerializerProto | None = EMPTY,
        provider: Provider | None = None,
        context: ContextRepo | None = None,
        # General:
        include_in_schema: bool = True,
        prefix: str = "",
    ) -> None:
        env = resolve_env_config(
            url=url,
            client_id=client_id,
            auth_token=auth_token,
            tls_enabled=tls_enabled,
            tls_cert_file=tls_cert_file,
            tls_key_file=tls_key_file,
            tls_ca_file=tls_ca_file,
            max_send_size=max_send_size,
            max_receive_size=max_receive_size,
            default_cq_timeout=default_cq_timeout,
        )

        # H-8: reject empty auth token
        resolved_token = env["auth_token"]
        if resolved_token is not None and resolved_token.strip() == "":
            raise ValueError(
                "KUBEMQ_AUTH_TOKEN is set but empty. "
                "Remove it for no auth, or provide a valid token."
            )

        validate_url(env["url"])

        if protocol is None:
            protocol = "kubemq+tls" if env["tls_enabled"] else "kubemq"

        spec_url: list[str]
        if specification_url is not None:
            spec_url = (
                [specification_url]
                if isinstance(specification_url, str)
                else list(specification_url)
            )
        else:
            spec_url = [env["url"]]

        super().__init__(
            routers=routers,
            config=KubeMQBrokerConfig(
                # KubeMQ-specific fields
                url=env["url"],
                client_id=re.sub(r"[^a-zA-Z0-9_-]", "-", env["client_id"] or socket.gethostname()),
                auth_token=resolved_token or "",
                tls_enabled=env["tls_enabled"],
                tls_cert_file=env["tls_cert_file"],
                tls_key_file=env["tls_key_file"],
                tls_ca_file=env["tls_ca_file"],
                max_send_size=env["max_send_size"],
                max_receive_size=env["max_receive_size"],
                default_cq_timeout=env["default_cq_timeout"],
                keepalive_time_ms=keepalive_time_ms,
                keepalive_timeout_ms=keepalive_timeout_ms,
                # BrokerConfig required fields (per KafkaBroker pattern)
                broker_decoder=decoder,
                broker_parser=parser,
                broker_middlewares=middlewares,
                logger=LoggerState(log_level=log_level),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                graceful_timeout=graceful_timeout,
                broker_dependencies=dependencies,
                extra_context={
                    "broker": self,
                },
                prefix=prefix,
                include_in_schema=include_in_schema,
            ),
            specification=BrokerSpec(
                description=description,
                url=spec_url,
                protocol=protocol,
                protocol_version=protocol_version or "1.0",
                security=security,
                tags=tags,
            ),
        )
        self._connection = None
        self._connect_lock = asyncio.Lock()

    async def __aenter__(self) -> Self:
        """Start broker (connect + launch subscriber consume loops)."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        """Connect and inject connection into all subscribers before starting."""
        conn = await self.connect()
        for sub in self.subscribers:
            if hasattr(sub, "set_connection"):
                sub.set_connection(conn)
        await super().start()

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,
    ) -> None:
        """Stop subscribers and close SDK connections."""
        await super().stop(exc_type, exc_val, exc_tb)
        if self._connection is not None:
            try:
                await self._connection.close()
            except Exception:
                logger.debug("Error closing KubeMQ connections", exc_info=True)
            self._connection = None
            self.config.broker_config.disconnect()

    async def _connect(self) -> KubeMQConnection:
        """Create 3 KubeMQ async clients and return the connection holder."""
        from kubemq.core.config import ClientConfig, KeepAliveConfig, TLSConfig
        from kubemq.cq import AsyncCQClient
        from kubemq.pubsub import AsyncPubSubClient
        from kubemq.queues import AsyncQueuesClient

        async with self._connect_lock:
            if self._connection is not None:
                return self._connection

            from pathlib import Path

            broker_config = self.config.broker_config
            address, tls_from_url = parse_kubemq_url(broker_config.url)
            tls_config = TLSConfig(
                enabled=tls_from_url or broker_config.tls_enabled,
                cert_file=Path(broker_config.tls_cert_file)
                if broker_config.tls_cert_file
                else None,
                key_file=Path(broker_config.tls_key_file) if broker_config.tls_key_file else None,
                ca_file=Path(broker_config.tls_ca_file) if broker_config.tls_ca_file else None,
            )
            keepalive = KeepAliveConfig(
                enabled=True,  # Always on; user cannot disable (documented behavior)
                ping_interval_in_seconds=broker_config.keepalive_time_ms // 1000,
                ping_timeout_in_seconds=broker_config.keepalive_timeout_ms // 1000,
                permit_without_calls=True,
            )
            sdk_config = ClientConfig(
                address=address,
                client_id=broker_config.client_id,
                auth_token=broker_config.auth_token or None,
                tls=tls_config,
                keep_alive=keepalive,
                max_send_size=broker_config.max_send_size,
                max_receive_size=broker_config.max_receive_size,
                connection_pool_size=1,  # H-10: prevent over-allocation
            )

            pubsub = AsyncPubSubClient(config=sdk_config)
            queues = AsyncQueuesClient(config=sdk_config)
            cq = AsyncCQClient(config=sdk_config)

            connected: list[Any] = []
            try:
                await pubsub.connect()
                connected.append(pubsub)
                await queues.connect()
                connected.append(queues)
                await cq.connect()
                connected.append(cq)
            except Exception:
                for client in reversed(connected):
                    try:
                        await client.close()
                    except Exception:
                        logger.debug(
                            "Error closing client during connect cleanup",
                            exc_info=True,
                        )
                raise

            logger.info("Connected to KubeMQ at %s", address)
            conn = KubeMQConnection(pubsub=pubsub, queues=queues, cq=cq)
            producer = KubeMQProducer(connection=conn)
            broker_config.connect(conn, producer)
            self._connection = conn
            return conn

    async def ping(self, timeout: float | None = 5.0) -> bool:
        """Health check via KubeMQ Ping RPC (H-9 fix)."""
        sleep_time = (timeout or 10) / 10
        with anyio.move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False
            while True:
                if cancel_scope.cancel_called:
                    return False
                try:
                    result = await self._connection.pubsub.ping()
                    if result is not None:
                        return True
                except Exception:
                    pass
                await anyio.sleep(sleep_time)
        return False

    async def publish(  # type: ignore[override]
        self,
        message: SendableMessage,
        /,
        *,
        events: str | None = None,
        events_store: str | None = None,
        queues: str | None = None,
        commands: str | None = None,
        queries: str | None = None,
        headers: dict[str, str] | None = None,
        metadata: str = "",
        correlation_id: str | None = None,
        timeout: int | None = None,
        cache_key: str | None = None,
        cache_ttl: int | None = None,
        reply_to: str | None = None,
    ) -> Any:
        """Publish a message to a KubeMQ channel."""
        channel, pattern = _resolve_pattern(
            events=events,
            events_store=events_store,
            queues=queues,
            commands=commands,
            queries=queries,
        )
        cmd = KubeMQPublishCommand(
            message,
            destination=channel,
            pattern=pattern,
            headers=headers or {},
            metadata=metadata,
            correlation_id=correlation_id or "",
            reply_to=reply_to or "",
            timeout=timeout,
            cache_key=cache_key,
            cache_ttl=cache_ttl,
        )
        if self.config.producer is None:
            raise RuntimeError("Broker not connected. Call connect() or start() first.")
        return await self.config.producer.publish(cmd)

    async def request(  # type: ignore[override]
        self,
        message: SendableMessage,
        /,
        *,
        commands: str | None = None,
        queries: str | None = None,
        timeout: int | None = None,
        cache_key: str | None = None,
        cache_ttl: int | None = None,
        headers: dict[str, str] | None = None,
        metadata: str = "",
        correlation_id: str | None = None,
    ) -> Any:
        """Send a command or query request."""
        if commands is None and queries is None:
            raise FeatureNotSupportedException("request() requires commands= or queries=")
        channel, pattern = _resolve_pattern(commands=commands, queries=queries)
        cmd = KubeMQPublishCommand(
            message,
            destination=channel,
            pattern=pattern,
            headers=headers or {},
            metadata=metadata,
            correlation_id=correlation_id or "",
            timeout=timeout or self.config.broker_config.default_cq_timeout,
            cache_key=cache_key,
            cache_ttl=cache_ttl,
        )
        if self.config.producer is None:
            raise RuntimeError("Broker not connected. Call connect() or start() first.")
        return await self.config.producer.request(cmd)

    async def publish_batch(  # type: ignore[override]
        self,
        *messages: SendableMessage,
        queues: str,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Publish a batch of queue messages."""
        from faststream.message import encode_message

        encoded = [encode_message(m, None)[0] for m in messages]
        cmd = KubeMQPublishCommand(
            b"",
            destination=queues,
            pattern=KubeMQPattern.QUEUES,
            headers=headers or {},
        )
        cmd._batch_bodies = tuple(encoded)
        if self.config.producer is None:
            raise RuntimeError("Broker not connected. Call connect() or start() first.")
        return await self.config.producer.publish_batch(cmd)
