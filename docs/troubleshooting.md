# Troubleshooting

## My consumer never receives records

- Is the component's module registered (pico-boot discovery or explicit
  `init(modules=[...])`)?
- Verify `kafka.bootstrap_servers` points at the right cluster and the topic
  exists (or auto-creation is enabled broker-side).
- `kafka.enabled: false` silently disables everything.

## Both my consumers get every record / only one gets each

Group semantics: different `group_id`s = fan-out (each group sees every
record); same `group_id` = shared partition load. Set `group_id` per method
accordingly; the default comes from `kafka.group_id`.

## Records vanish when my handler raises

By design: logged and skipped, offsets advance. Reprocess selectively after
fixing the bug; for per-record retries, decorate the handler with
`@retryable` from pico-resilience.

## Produce raises RuntimeError: pico-kafka is disabled

`kafka.enabled` is false in this environment — loud on purpose.

## Sync produce blocks too long / times out

The stub waits for the broker ack, bounded by
`kafka.produce_timeout_seconds` (default 10). Check cluster reachability and
topic health first.

## Consumers stopped after container.shutdown()

That is the lifecycle: tasks cancel, clients stop, the loop dies with the
container.
