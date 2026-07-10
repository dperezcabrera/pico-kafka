"""Marker decorators for the two sides of Kafka messaging.

Consumer side: ``@kafka_consumer`` on a component method subscribes it
to a topic; the method receives the JSON-decoded record value.

Producer side: ``@kafka_producer`` on a class of ``@produce`` stubs
turns it into an injectable component whose methods send their
``message`` argument as a JSON value (same generated-implementation
idiom as pico-httpx clients and pico-rabbitmq publishers).
"""

import functools
import inspect

from pico_ioc import component

CONSUMER_META = "_pico_kafka_consumer_meta"
PRODUCE_META = "_pico_kafka_produce_meta"
PRODUCER_META = "_pico_kafka_producer_meta"


def kafka_consumer(topic: str, *, group_id: str = ""):
    """Subscribe a component method to ``topic``.

    ``group_id`` defaults to the ``kafka.group_id`` setting; give each
    independent projection its own group to fan the stream out.
    """
    if not topic:
        raise ValueError("@kafka_consumer requires a topic")

    def dec(fn):
        setattr(fn, CONSUMER_META, {"topic": topic, "group_id": group_id})
        return fn

    return dec


def kafka_producer(cls):
    """Turn a class of ``@produce`` stubs into an injectable component."""
    setattr(cls, PRODUCER_META, True)
    if "__init__" not in cls.__dict__:

        def __init__(self, registrar):
            self._pico_kafka = registrar

        from .registrar import KafkaRegistrar

        __init__.__annotations__ = {"registrar": KafkaRegistrar}
        cls.__init__ = __init__
    return component(cls)


def produce(topic: str):
    """Generate a producing implementation for a stub method.

    The stub's ``message`` argument (any JSON-serializable value) is the
    record value. Sync stubs block until the broker acks; async stubs
    await it.
    """
    if not topic:
        raise ValueError("@produce requires a topic")

    def dec(fn):
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_impl(self, message):
                import asyncio

                future = self._pico_kafka.produce(topic, message)
                return await asyncio.wrap_future(future)

            setattr(async_impl, PRODUCE_META, {"topic": topic})
            return async_impl

        @functools.wraps(fn)
        def sync_impl(self, message):
            future = self._pico_kafka.produce(topic, message)
            return future.result(timeout=self._pico_kafka.produce_timeout)

        setattr(sync_impl, PRODUCE_META, {"topic": topic})
        return sync_impl

    return dec
