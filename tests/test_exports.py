"""The public API is exactly what ``__all__`` declares (stability contract)."""

import pico_kafka


def test_public_api_is_declared_and_importable():
    assert set(pico_kafka.__all__) == {"KafkaRegistrar", "KafkaSettings", "kafka_consumer", "kafka_producer", "produce"}
    for name in pico_kafka.__all__:
        assert getattr(pico_kafka, name) is not None
