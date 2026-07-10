# Architecture

```
@kafka_consumer(topic, group_id=)        @kafka_producer + @produce stubs
        |                                          |
   marks method meta                 generated impl -> registrar.produce()
        |                                          |
KafkaRegistrar (@component) -----------------------+
   @configure: discover consumers -> one AIOKafkaConsumer task per method
   @cleanup:  cancel tasks, stop consumers + producer, stop loop
        |
dedicated asyncio loop in a daemon thread ("pico-kafka")
   per record: container.aget(cls) -> method(json.loads(record.value))
```

## Design decisions

- **Same shape as pico-rabbitmq on purpose**: dedicated daemon-thread loop,
  marker + registrar discovery, container resolution per record, generated
  producer stubs. Learning one messaging module teaches both; switching
  brokers costs little code.
- **One consumer task per subscribed method**: each `@kafka_consumer` gets
  its own `AIOKafkaConsumer` and asyncio task. Two methods on the same topic
  with different `group_id`s fan the stream out; same group shares partition
  load — plain Kafka semantics, no abstraction on top.
- **Skip-on-failure**: a raising handler is logged and its record skipped so
  offsets keep advancing. The alternative — blocking the partition on a
  poison record — halts every record behind it. Replay is an operational
  decision, made with the traceback in hand.
- **Lazy producer**: `AIOKafkaProducer` starts on first produce and stops
  with the container. Consumer-less publisher apps pay nothing at boot.
- **JSON-only values, no keys/headers in 0.1**: covers the ecosystem's
  common case; keyed or headered records use aiokafka directly.
