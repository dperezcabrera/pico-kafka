# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-07-10

### Fixed

- `stop()` claims the loop and thread atomically under the registrar lock, so concurrent stops (ASGI lifespan plus a manual `container.shutdown()`) can no longer interleave — the loser sees no loop and returns. This was the root cause of the level-2 teardown hang that 0.1.3 only bounded; defense in depth alongside pico-ioc 2.3.3's idempotent container shutdown. Announced in 0.1.4 but that build shipped without the code change; the regression test now drives a real loop and fails against the 0.1.4 `stop()`.

## [0.1.4] - 2026-07-10

### Added

- Concurrent-stop regression test. The atomic `stop()` it was meant to pin missed this build (the test passed vacuously without a running loop); fixed for real in 0.1.5.

## [0.1.3] - 2026-07-10

### Fixed

- `stop()` is best-effort: if the clean shutdown exceeds its deadline, it logs a warning and forces the loop down instead of raising through `container.shutdown()`. An unclean broker disconnect beats a bricked application exit.

## [0.1.2] - 2026-07-10

### Fixed

- Shutdown uses `consumer_start_timeout_seconds` as its deadline too: stopping real consumers (group leave + offset commit) under load exceeded the 10s produce timeout and made `container.shutdown()` raise. Found by the flagship level-2 teardown.

## [0.1.1] - 2026-07-10

### Fixed

- Consumer startup no longer reuses `produce_timeout_seconds` as its deadline: new `kafka.consumer_start_timeout_seconds` setting (default 60s). Cold brokers (fresh KRaft clusters) exposing the group coordinator slowly used to kill the whole container boot at 10s.

## [0.1.0] - 2026-07-10

### Added

- `@kafka_consumer(topic, group_id=)` marker for component methods (sync and async); JSON record values decoded before dispatch.
- `@kafka_producer` / `@produce(topic)` declarative producing clients (sync stubs block on ack, async stubs await).
- `KafkaRegistrar`: dedicated background asyncio loop; one aiokafka consumer task per subscribed method; skip-on-failure policy so poison records never stall a partition; everything stopped on container shutdown.
- Settings under the `kafka` prefix: `bootstrap_servers`, `enabled`, `group_id`, `produce_timeout_seconds` (zero-config).
- Auto-discovery via the `pico_boot.modules` entry point.
