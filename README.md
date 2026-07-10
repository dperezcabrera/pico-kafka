# pico-kafka

[![PyPI version](https://img.shields.io/pypi/v/pico-kafka.svg)](https://pypi.org/project/pico-kafka/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/dperezcabrera/pico-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/dperezcabrera/pico-kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/dperezcabrera/pico-kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/dperezcabrera/pico-kafka)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue)](https://dperezcabrera.github.io/pico-kafka/)

Kafka for the [pico ecosystem](https://github.com/dperezcabrera/pico-ioc): `@kafka_consumer` methods and declarative `@kafka_producer` clients over aiokafka.

## Installation

```bash
pip install pico-kafka
```

## Quick start

```yaml
kafka:
  bootstrap_servers: kafka.internal:9092
  group_id: myapp
```

Consume — a component method per topic, JSON value decoded for you:

```python
from pico_ioc import component
from pico_kafka import kafka_consumer

@component
class OrderProjection:
    @kafka_consumer("orders")
    async def on_order(self, message: dict):
        ...

    @kafka_consumer("orders", group_id="analytics")   # independent fan-out
    def on_order_analytics(self, message: dict):
        ...
```

Produce — stubs, like a pico-httpx client:

```python
from pico_kafka import kafka_producer, produce

@kafka_producer
class OrderEvents:
    @produce("orders")
    def order_created(self, message): ...
```

Semantics:

- Consumers and producers run on a dedicated background loop — works in sync scripts, FastAPI apps and workers alike, no lifespan wiring.
- Each record resolves its component through the container (prototype scope = fresh instance per record).
- A record whose handler raises is logged and **skipped** — offsets advance, a poison record cannot stall the partition.
- Sync produce stubs block until the broker acks; async stubs await it. Everything stops with the container.

## Documentation

Full documentation: https://dperezcabrera.github.io/pico-kafka/

## License

MIT
