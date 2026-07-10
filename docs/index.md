# pico-kafka

Kafka on pico components: `@kafka_consumer` methods, declarative `@kafka_producer` clients.

## Install

```bash
pip install pico-kafka
```

## 30-second example

```python
from pico_ioc import component
from pico_kafka import kafka_consumer, kafka_producer, produce

@component
class OrderProjection:
    @kafka_consumer("orders")
    async def on_order(self, message: dict):
        ...

@kafka_producer
class OrderEvents:
    @produce("orders")
    def order_created(self, message): ...
```

No wiring: the module subscribes at startup, dispatches JSON record values to your methods and stops with the container.
