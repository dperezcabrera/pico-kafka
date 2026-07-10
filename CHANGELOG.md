# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
