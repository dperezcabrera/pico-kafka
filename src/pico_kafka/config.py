"""Settings for pico-kafka (prefix ``kafka``, zero-config)."""

from dataclasses import dataclass

from pico_ioc import configured


@configured(target="self", prefix="kafka", mapping="tree")
@dataclass
class KafkaSettings:
    """``enabled: false`` disables consumers and producers entirely."""

    bootstrap_servers: str = "localhost:9092"
    enabled: bool = True
    group_id: str = "pico"
    produce_timeout_seconds: float = 10.0
