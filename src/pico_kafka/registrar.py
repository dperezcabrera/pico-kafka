"""Connection owner and dispatcher.

Runs a dedicated asyncio loop in a daemon thread (same architecture as
pico-rabbitmq): consumers and producers work in any app without
lifespan wiring. Each ``@kafka_consumer`` method gets its own
``AIOKafkaConsumer`` task; records resolve their component through the
container per message.

Failure policy: a consumer method that raises is logged and its record
is skipped — offsets keep advancing so a poison record cannot stall the
partition. Reprocess from your monitoring, not from a hot loop.
"""

import asyncio
import concurrent.futures
import inspect
import json
import logging
import threading
from typing import Any

import aiokafka
from pico_ioc import PicoContainer, cleanup, component, configure

from .config import KafkaSettings
from .decorators import CONSUMER_META

logger = logging.getLogger(__name__)


@component
class KafkaRegistrar:
    def __init__(self, container: PicoContainer, settings: KafkaSettings):
        self._container = container
        self._settings = settings
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._producer = None
        self._tasks: list = []
        self._consumers: list = []
        self._lock = threading.Lock()

    @property
    def produce_timeout(self) -> float:
        return self._settings.produce_timeout_seconds

    # ── lifecycle ────────────────────────────────────────────────

    @configure
    def start(self) -> None:
        if not self._settings.enabled:
            return
        consumers = list(self._discover_consumers())
        if consumers:
            self._ensure_loop()
            self._run(self._start_consumers(consumers), timeout=self._settings.consumer_start_timeout_seconds)

    @cleanup
    def stop(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._shutdown(), timeout=self._settings.consumer_start_timeout_seconds)
        except TimeoutError:
            # Shutdown must NEVER hang the application: log and force the
            # loop down. Unclean broker disconnect beats a bricked exit.
            logger.warning(
                "kafka shutdown timed out after %.0fs; forcing loop stop", self._settings.consumer_start_timeout_seconds
            )
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()
        self._loop = None
        self._thread = None

    async def _shutdown(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        for consumer in self._consumers:
            await consumer.stop()
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    def _ensure_loop(self) -> None:
        with self._lock:
            if self._loop is not None:
                return
            self._loop = asyncio.new_event_loop()
            self._thread = threading.Thread(target=self._loop.run_forever, name="pico-kafka", daemon=True)
            self._thread.start()

    def _run(self, coro, timeout: float | None = None):
        limit = timeout if timeout is not None else self._settings.produce_timeout_seconds
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout=limit)

    # ── consumer side ────────────────────────────────────────────

    def _discover_consumers(self):
        locator = getattr(self._container, "_locator", None)
        metadata_map = getattr(locator, "_metadata", {}) if locator else {}
        for md in metadata_map.values():
            cls = getattr(md, "concrete_class", None)
            if not inspect.isclass(cls):
                continue
            for name, fn in inspect.getmembers(cls, inspect.isfunction):
                meta = getattr(fn, CONSUMER_META, None)
                if meta is not None:
                    yield cls, name, meta

    async def _start_consumers(self, consumers) -> None:
        for cls, method_name, meta in consumers:
            consumer = aiokafka.AIOKafkaConsumer(
                meta["topic"],
                bootstrap_servers=self._settings.bootstrap_servers,
                group_id=meta["group_id"] or self._settings.group_id,
            )
            await consumer.start()
            self._consumers.append(consumer)
            self._tasks.append(asyncio.ensure_future(self._consume(consumer, cls, method_name)))
            logger.info("consuming %s -> %s.%s", meta["topic"], cls.__name__, method_name)

    async def _consume(self, consumer, cls, method_name) -> None:
        async for record in consumer:
            try:
                body = json.loads(record.value)
                instance = await self._container.aget(cls)
                result = getattr(instance, method_name)(body)
                if inspect.iscoroutine(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                logger.exception("consumer %s.%s failed; record skipped", cls.__name__, method_name)

    # ── producer side ────────────────────────────────────────────

    def produce(self, topic: str, message: Any) -> concurrent.futures.Future:
        if not self._settings.enabled:
            raise RuntimeError("pico-kafka is disabled (kafka.enabled=false)")
        self._ensure_loop()
        return asyncio.run_coroutine_threadsafe(self._do_produce(topic, message), self._loop)

    async def _do_produce(self, topic: str, message: Any) -> None:
        if self._producer is None:
            self._producer = aiokafka.AIOKafkaProducer(bootstrap_servers=self._settings.bootstrap_servers)
            await self._producer.start()
        value = json.dumps(message, ensure_ascii=False).encode("utf-8")
        await self._producer.send_and_wait(topic, value)
