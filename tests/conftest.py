"""In-memory fake of the aiokafka surface pico-kafka uses.

Tests exercise our glue (discovery, dispatch, skip-on-failure policy,
producer stubs) against a broker-less fake; aiokafka itself is trusted.
"""

import asyncio
import json

import pytest
from pico_ioc import DictSource, configuration, init

import pico_kafka.registrar as registrar_module


class FakeRecord:
    def __init__(self, value: bytes):
        self.value = value


class FakeConsumer:
    def __init__(self, bus, topic, group_id):
        self._bus = bus
        self.topic = topic
        self.group_id = group_id
        self.queue = None
        self.started = False
        self.stopped = False

    async def start(self):
        self.queue = asyncio.Queue()
        self.started = True
        self._bus.consumers.setdefault(self.topic, []).append(self)

    async def stop(self):
        self.stopped = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.stopped:
            raise StopAsyncIteration
        return await self.queue.get()


class FakeProducer:
    def __init__(self, bus, bootstrap_servers):
        self._bus = bus
        self.bootstrap_servers = bootstrap_servers
        self.started = False
        self.stopped = False
        self.sent = []

    async def start(self):
        self.started = True
        self._bus.producer = self

    async def stop(self):
        self.stopped = True

    async def send_and_wait(self, topic, value):
        self.sent.append((topic, value))
        for consumer in self._bus.consumers.get(topic, []):
            await consumer.queue.put(FakeRecord(value))


class FakeBus:
    def __init__(self):
        self.consumers = {}
        self.producer = None
        self.loop = None

    def AIOKafkaConsumer(self, topic, *, bootstrap_servers, group_id):
        self.bootstrap_servers = bootstrap_servers
        return FakeConsumer(self, topic, group_id)

    def AIOKafkaProducer(self, *, bootstrap_servers):
        return FakeProducer(self, bootstrap_servers)


@pytest.fixture(autouse=True)
def isolate_from_installed_plugins(monkeypatch):
    monkeypatch.setenv("PICO_BOOT_AUTO_PLUGINS", "false")


@pytest.fixture
def bus(monkeypatch):
    fake = FakeBus()
    monkeypatch.setattr(registrar_module, "aiokafka", fake)
    return fake


@pytest.fixture
def deliver(bus):
    """Push a record into every consumer of a topic and wait until each
    consumer task has looped back to waiting (record fully processed)."""

    def _deliver(topic: str, payload):
        value = json.dumps(payload).encode()

        async def _push_and_drain():
            for consumer in bus.consumers.get(topic, []):
                await consumer.queue.put(FakeRecord(value))
            for consumer in bus.consumers.get(topic, []):
                while not consumer.queue.empty():
                    await asyncio.sleep(0.001)
                await asyncio.sleep(0.01)

        asyncio.run_coroutine_threadsafe(_push_and_drain(), bus.loop).result(timeout=5)

    return _deliver


@pytest.fixture
def make_container(bus):
    created = []

    def _make(*modules, config=None):
        cfg = configuration(DictSource(config or {}))
        container = init(modules=["pico_kafka", *modules], config=cfg)
        created.append(container)
        from pico_kafka import KafkaRegistrar

        bus.loop = container.get(KafkaRegistrar)._loop
        return container

    yield _make
    for c in reversed(created):
        c.shutdown()
