import sys

import pytest
from pico_ioc import component

from pico_kafka import KafkaRegistrar, kafka_consumer, kafka_producer, produce


@component
class OrderProjection:
    seen = []
    async_seen = []

    @kafka_consumer("orders")
    def on_order(self, message: dict):
        OrderProjection.seen.append(message)

    @kafka_consumer("orders-analytics", group_id="analytics")
    async def on_order_async(self, message: dict):
        OrderProjection.async_seen.append(message)


@component
class Exploder:
    attempts = 0
    after = []

    @kafka_consumer("poison")
    def on_message(self, message: dict):
        Exploder.attempts += 1
        if message.get("bad"):
            raise RuntimeError("boom")
        Exploder.after.append(message)


@kafka_producer
class OrderEvents:
    @produce("orders")
    def order_created(self, message): ...

    @produce("orders-analytics")
    async def order_analytics(self, message): ...


def test_consumer_receives_json(make_container, bus, deliver):
    OrderProjection.seen = []
    make_container(sys.modules[__name__])
    deliver("orders", {"id": 1})
    assert OrderProjection.seen == [{"id": 1}]


def test_async_consumer_and_custom_group(make_container, bus, deliver):
    OrderProjection.async_seen = []
    make_container(sys.modules[__name__])
    consumer = bus.consumers["orders-analytics"][0]
    assert consumer.group_id == "analytics"
    deliver("orders-analytics", {"id": 2})
    assert OrderProjection.async_seen == [{"id": 2}]


def test_default_group_comes_from_settings(make_container, bus):
    make_container(sys.modules[__name__], config={"kafka": {"group_id": "myapp"}})
    assert bus.consumers["orders"][0].group_id == "myapp"


def test_failing_record_is_skipped_not_fatal(make_container, bus, deliver):
    Exploder.attempts = 0
    Exploder.after = []
    make_container(sys.modules[__name__])
    deliver("poison", {"bad": True})
    deliver("poison", {"bad": False, "id": 3})
    assert Exploder.attempts == 2
    assert Exploder.after == [{"bad": False, "id": 3}]


def test_sync_produce_reaches_consumer(make_container, bus):
    OrderProjection.seen = []
    container = make_container(sys.modules[__name__])
    container.get(OrderEvents).order_created({"id": 4})
    assert bus.producer.sent[0][0] == "orders"
    import time

    deadline = time.monotonic() + 3
    while not OrderProjection.seen and time.monotonic() < deadline:
        time.sleep(0.01)
    assert OrderProjection.seen == [{"id": 4}]


@pytest.mark.asyncio
async def test_async_produce(make_container, bus):
    container = make_container(sys.modules[__name__])
    await container.get(OrderEvents).order_analytics({"id": 5})
    assert bus.producer.sent[0][0] == "orders-analytics"


def test_shutdown_stops_consumers_and_producer(make_container, bus):
    container = make_container(sys.modules[__name__])
    container.get(OrderEvents).order_created({"id": 6})
    producer = bus.producer
    container.shutdown()
    assert producer.stopped is True
    assert all(c.stopped for consumers in bus.consumers.values() for c in consumers)


def test_disabled_starts_nothing(make_container, bus):
    container = make_container(sys.modules[__name__], config={"kafka": {"enabled": False}})
    assert bus.consumers == {}
    with pytest.raises(RuntimeError, match="disabled"):
        container.get(OrderEvents).order_created({"id": 7})


def test_producer_without_consumers_connects_lazily(make_container, bus):
    container = make_container()

    @kafka_producer
    class Lone:
        @produce("solo")
        def send(self, message): ...

    registrar = container.get(KafkaRegistrar)
    assert bus.producer is None
    Lone(registrar).send({"id": 8})
    assert bus.producer.sent == [("solo", b'{"id": 8}')]


def test_producer_with_custom_init(make_container, bus):
    @kafka_producer
    class Custom:
        def __init__(self, registrar: KafkaRegistrar):
            self._pico_kafka = registrar
            self.ready = True

    container = make_container()
    assert Custom(container.get(KafkaRegistrar)).ready is True


def test_consumer_requires_topic():
    with pytest.raises(ValueError, match="topic"):
        kafka_consumer("")


def test_produce_requires_topic():
    with pytest.raises(ValueError, match="topic"):
        produce("")


@component
class Canceller:
    @kafka_consumer("cancelme")
    def on_message(self, message: dict):
        import asyncio

        raise asyncio.CancelledError()


def test_cancellation_during_processing_kills_task_quietly(make_container, bus, deliver):
    container = make_container(sys.modules[__name__])
    deliver("cancelme", {})
    registrar = container.get(KafkaRegistrar)
    import time

    deadline = time.monotonic() + 3
    while not any(t.cancelled() or t.done() for t in registrar._tasks) and time.monotonic() < deadline:
        time.sleep(0.01)
    assert any(t.done() for t in registrar._tasks)


def test_producer_is_reused_across_produces(make_container, bus):
    container = make_container(sys.modules[__name__])
    events = container.get(OrderEvents)
    events.order_created({"id": 1})
    first = bus.producer
    events.order_created({"id": 2})
    assert bus.producer is first
    assert len(first.sent) == 2


def test_slow_broker_startup_survives_short_produce_timeout(make_container, bus, monkeypatch):
    import asyncio as aio

    original_start = type(bus).AIOKafkaConsumer

    def slow_consumer(self, topic, *, bootstrap_servers, group_id):
        consumer = original_start(self, topic, bootstrap_servers=bootstrap_servers, group_id=group_id)
        real_start = consumer.start

        async def delayed_start():
            await aio.sleep(0.5)
            await real_start()

        consumer.start = delayed_start
        return consumer

    monkeypatch.setattr(type(bus), "AIOKafkaConsumer", slow_consumer)
    container = make_container(
        sys.modules[__name__],
        config={"kafka": {"produce_timeout_seconds": 0.1, "consumer_start_timeout_seconds": 30}},
    )
    assert bus.consumers  # el arranque sobrevivio a un broker mas lento que produce_timeout


def test_slow_consumer_stop_survives_short_produce_timeout(make_container, bus, monkeypatch):
    import asyncio as aio

    container = make_container(
        sys.modules[__name__],
        config={"kafka": {"produce_timeout_seconds": 0.1, "consumer_start_timeout_seconds": 30}},
    )
    for consumers in bus.consumers.values():
        for consumer in consumers:
            real_stop = consumer.stop

            async def slow_stop(_real=real_stop):
                await aio.sleep(0.3)
                await _real()

            consumer.stop = slow_stop
    container.shutdown()  # no debe lanzar TimeoutError


def test_hanging_consumer_stop_never_bricks_shutdown(make_container, bus, caplog):
    import asyncio as aio

    container = make_container(
        sys.modules[__name__],
        config={"kafka": {"consumer_start_timeout_seconds": 0.3}},
    )
    for consumers in bus.consumers.values():
        for consumer in consumers:

            async def hang_forever():
                await aio.sleep(3600)

            consumer.stop = hang_forever
    with caplog.at_level("WARNING"):
        container.shutdown()  # completa con warning, jamas cuelga ni lanza
    assert "forcing loop stop" in caplog.text


def test_concurrent_stop_is_safe(make_container, bus):
    import asyncio
    import threading

    container = make_container(sys.modules[__name__])
    registrar = container.get(KafkaRegistrar)
    # loop real corriendo: sin el, el test pasa en vacio (stop retorna en el guard)
    registrar._loop = asyncio.new_event_loop()
    registrar._thread = threading.Thread(target=registrar._loop.run_forever, daemon=True)
    registrar._thread.start()
    loop = registrar._loop

    errors = []

    def stop():
        try:
            registrar.stop()
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=stop) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    registrar.stop()
    assert errors == []
    assert registrar._loop is None
    assert loop.is_closed()
