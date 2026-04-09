import json

import pytest

import mqtt_handler
from ._mqtt_test_helpers import DummyClient


def _make_handler(monkeypatch):
    # Ensure HomeNodeMQTT() uses our dummy client
    monkeypatch.setattr(mqtt_handler.mqtt, "Client", lambda *a, **k: DummyClient())

    # Keep discovery IDs deterministic for assertions
    monkeypatch.setattr(mqtt_handler.config, "ID_SUFFIX", "_T", raising=False)
    monkeypatch.setattr(mqtt_handler.config, "BRIDGE_NAME", "rtl-haos-bridge", raising=False)
    monkeypatch.setattr(mqtt_handler.config, "BRIDGE_ID", "42", raising=False)
    monkeypatch.setattr(mqtt_handler.config, "RTL_EXPIRE_AFTER", 60, raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    return h, h.client


def _last_published_json(client, topic_prefix):
    matches = [(t, p, r) for (t, p, r) in client.published if t.startswith(topic_prefix)]
    assert matches, f"Expected publish to {topic_prefix}, got: {client.published}"
    t, payload, _retain = matches[-1]
    return t, json.loads(payload)


@pytest.mark.parametrize(
    "sensor_name, expected_device_class, expected_unit, expected_state_class",
    [
        ("pressure_hPa", "pressure", "hPa", "measurement"),
        ("wind_avg_m_s", "wind_speed", "m/s", "measurement"),
        ("energy_kWh", "energy", "kWh", "total_increasing"),
    ],
)
def test_publish_discovery_uses_field_meta_for_new_fields(monkeypatch, sensor_name, expected_device_class, expected_unit, expected_state_class):
    h, c = _make_handler(monkeypatch)

    h._publish_discovery(
        sensor_name=sensor_name,
        state_topic=f"home/rtl_devices/dev/{sensor_name}",
        unique_id=f"dev_{sensor_name}",
        device_name="My Device",
        device_model="SomeModel",
        compound_id="rtl433_SomeModel_dev",
    )

    topic, payload = _last_published_json(c, f"homeassistant/sensor/dev_{sensor_name}_T/config")
    assert payload["device_class"] == expected_device_class
    assert payload["unit_of_measurement"] == expected_unit
    assert payload["state_class"] == expected_state_class

    # Ensure icon and friendly name come from field_meta, not fallbacks.
    assert payload.get("icon") == mqtt_handler.FIELD_META[sensor_name][2]
    assert payload.get("name") == mqtt_handler.FIELD_META[sensor_name][3]


def test_battery_pct_does_not_force_state_class(monkeypatch):
    h, c = _make_handler(monkeypatch)

    h._publish_discovery(
        sensor_name="battery_pct",
        state_topic="home/rtl_devices/dev/battery_pct",
        unique_id="dev_battery_pct",
        device_name="My Device",
        device_model="SomeModel",
        compound_id="rtl433_SomeModel_dev",
    )

    _topic, payload = _last_published_json(c, "homeassistant/sensor/dev_battery_pct_T/config")
    assert payload["device_class"] == "battery"
    assert payload["unit_of_measurement"] == "%"
    assert "state_class" not in payload
