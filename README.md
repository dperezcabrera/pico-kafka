# pico-kafka

[![PyPI](https://img.shields.io/pypi/v/pico-kafka.svg)](https://pypi.org/project/pico-kafka/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/dperezcabrera/pico-kafka)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
![CI (tox matrix)](https://github.com/dperezcabrera/pico-kafka/actions/workflows/ci.yml/badge.svg)
[![codecov](https://codecov.io/gh/dperezcabrera/pico-kafka/branch/main/graph/badge.svg)](https://codecov.io/gh/dperezcabrera/pico-kafka)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dperezcabrera_pico-kafka&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=dperezcabrera_pico-kafka)
[![Duplicated Lines (%)](https://sonarcloud.io/api/project_badges/measure?project=dperezcabrera_pico-kafka&metric=duplicated_lines_density)](https://sonarcloud.io/summary/new_code?id=dperezcabrera_pico-kafka)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=dperezcabrera_pico-kafka&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=dperezcabrera_pico-kafka)
[![PyPI Downloads](https://img.shields.io/pypi/dm/pico-kafka)](https://pypi.org/project/pico-kafka/)
[![Docs](https://img.shields.io/badge/Docs-pico--kafka-blue?style=flat&logo=readthedocs&logoColor=white)](https://dperezcabrera.github.io/pico-kafka/)
[![Interactive Lab](https://img.shields.io/badge/Learn-online-green?style=flat&logo=python&logoColor=white)](https://dperezcabrera.github.io/pico-learn/)

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
