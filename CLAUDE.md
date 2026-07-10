Read and follow ./AGENTS.md for project conventions.

## Pico Ecosystem Context

pico-kafka — Kafka over aiokafka: @kafka_consumer methods and @kafka_producer/@produce declarative clients. Auto-discovered via the `pico_boot.modules` entry point. See it wired with the whole ecosystem in the flagship use case (pico-boot docs).

## Key Reminders

- pico-ioc dependency: `>= 2.2.0`; aiokafka `>= 0.11`
- **NEVER change `version_scheme`** in pyproject.toml. It MUST remain `"post-release"`.
- requires-python >= 3.11
- Commit messages: one line only
