# Getting Started

## Prerequisites

- Python >= 3.11
- pico-ioc >= 2.2.0 (pico-boot recommended for auto-discovery)
- aiokafka >= 0.11 (installed automatically)
- A reachable Kafka cluster

## Install

```bash
pip install pico-kafka
```

## Key concepts

| Piece | What it does |
|---|---|
| `@kafka_consumer(topic, group_id=)` | Subscribes a component method to a topic; JSON value decoded to the `message` argument |
| `@kafka_producer` + `@produce(topic)` | Class of stubs whose methods send their `message` argument as a JSON value |
| `KafkaRegistrar` | Owns the aiokafka clients on a dedicated background loop; starts and stops with the container |
| `kafka.bootstrap_servers` | Cluster address (default `localhost:9092`) |
| `kafka.group_id` | Default consumer group (default `pico`) |

## Consuming

```python
@component
class OrderProjection:
    @kafka_consumer("orders")
    async def on_order(self, message: dict):
        ...

    @kafka_consumer("orders", group_id="analytics")
    def on_order_analytics(self, message: dict):
        ...
```

- Two methods on the same topic with different `group_id`s each see every record (fan-out); with the same group they share the partition load.
- Sync and async methods both work; each record resolves the component through the container.
- **Failure policy**: a handler that raises is logged and the record skipped — offsets advance, a poison record cannot stall the partition. Replay from your monitoring when needed.

## Producing

```python
@kafka_producer
class OrderEvents:
    @produce("orders")
    def order_created(self, message): ...

    @produce("orders")
    async def order_created_async(self, message): ...
```

Sync stubs block until the broker acks (bounded by `kafka.produce_timeout_seconds`); async stubs await it. The producer starts lazily on first use and stops with the container.

## Disabling

```yaml
kafka:
  enabled: false
```

Consumers do not start and produce attempts raise `RuntimeError` — loud, because a silently dropped event is worse than a crash.
