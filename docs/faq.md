# FAQ

## When pico-rabbitmq and when pico-kafka?

Same decision as RabbitMQ vs Kafka. RabbitMQ: routing-rich work distribution and transient events. Kafka: ordered, replayable streams with consumer groups and retention. The pico surface is deliberately symmetric so switching costs little code.

## Why are failing records skipped instead of retried?

Blocking a partition on a deterministically failing record halts every record behind it. Skipping with a logged traceback keeps the stream moving; replay selectively when you have fixed the bug. If you need per-record retries, do them inside the handler (`@retryable` from pico-resilience composes).

## What about keys, partitions and headers?

Not in 0.1 — `@produce` sends the JSON value only. For keyed or headered records use aiokafka's producer directly; the declarative stubs cover the common case.

## Does it block my event loop?

No. The module runs its own loop in a daemon thread. Sync produce stubs block only the calling thread; async stubs await without blocking.

## How do I test my consumers?

Call the method with a dict — it is a plain component method. The Kafka plumbing is pico-kafka's job, tested here.
