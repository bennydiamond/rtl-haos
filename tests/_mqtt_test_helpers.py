"""Shared helpers for MQTT-centric tests.

Kept in a non-test module so pytest doesn't try to collect it.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, List, Tuple


@dataclass
class DummyClient:
    """Minimal paho-mqtt Client stub capturing publishes/subscribes."""

    published: List[Tuple[str, str, bool]] = field(default_factory=list)
    subscribed: List[Tuple[Any, ...]] = field(default_factory=list)

    def __init__(self, *a: Any, **k: Any) -> None:
        self.published = []
        self.subscribed = []

    def username_pw_set(self, *_a: Any, **_k: Any) -> None:
        return None

    def will_set(self, *_a: Any, **_k: Any) -> None:
        return None

    def publish(self, topic: str, payload: str, retain: bool = False):
        self.published.append((topic, payload, retain))
        # paho returns an MQTTMessageInfo-like object; tests don't need it.
        return None

    def subscribe(self, *args: Any, **kwargs: Any):
        self.subscribed.append(tuple(args))
        return None


def last_published(client: DummyClient, topic: str) -> Tuple[str, str, bool]:
    matches = [tup for tup in client.published if tup[0] == topic]
    assert matches, f"Expected at least one publish to {topic}"
    return matches[-1]


def last_state_payload(client: DummyClient, compound_id: str, field: str) -> str:
    topic = f"home/rtl_devices/{compound_id}/{field}"
    _t, payload, _r = last_published(client, topic)
    return payload


def last_discovery_payload(
    client: DummyClient,
    *,
    domain: str,
    unique_id_with_suffix: str,
) -> dict:
    topic = f"homeassistant/{domain}/{unique_id_with_suffix}/config"
    _t, payload, _r = last_published(client, topic)
    return json.loads(payload)


def assert_float_str(value: str, expected: float, *, rel: float = 1e-9, abs_tol: float = 1e-9) -> None:
    """Parse a numeric MQTT payload and compare with tolerance.

    This avoids brittle tests that depend on exact float->str formatting.
    """
    try:
        got = float(value)
    except Exception as e:  # pragma: no cover
        raise AssertionError(f"Expected numeric string, got {value!r}") from e

    # Manual approx to avoid importing pytest in helper module.
    delta = abs(got - expected)
    tol = max(abs_tol * 1.0, rel * max(abs(got), abs(expected)))
    assert delta <= tol, f"{got} != {expected} (delta={delta}, tol={tol})"
