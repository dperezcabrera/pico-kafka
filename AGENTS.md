# pico-kafka

Kafka over aiokafka: @kafka_consumer methods and @kafka_producer/@produce declarative clients.

## Commands

```bash
pip install -e ".[dev]"
pytest tests/ -v
pytest --cov=pico_kafka --cov-report=term-missing tests/
mkdocs serve -f mkdocs.yml
```

## Project Structure

```
src/pico_kafka/
  __init__.py       # Public API
  decorators.py     # @kafka_consumer marker; @kafka_producer class + @produce stub->impl
  registrar.py      # KafkaRegistrar: daemon-thread loop, one consumer task per method
  config.py         # KafkaSettings (prefix "kafka")
```

## Key Concepts

- Same architecture as pico-rabbitmq (daemon-thread loop, marker+registrar, generated stubs) on purpose.
- One AIOKafkaConsumer per subscribed method; different group_ids on a topic = fan-out, same group = shared partitions.
- Skip-on-failure: a raising handler logs and the record is SKIPPED (offsets advance; poison records never stall a partition).
- Lazy producer; JSON values only (no keys/headers in 0.1).
- Known limit: consumer startup reuses produce_timeout (10s); slow broker kills boot — own startup timeout pending.

## Boundaries

- Kafka semantics stay visible (groups, offsets) — no abstraction on top
- Replay/retry of failed records is the operator's decision, not the module's
- Do not modify `_version.py`
