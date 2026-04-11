import json

import config
import mqtt_handler

from ._mqtt_test_helpers import DummyClient, last_published


def _patch_common(monkeypatch):
    monkeypatch.setattr(mqtt_handler.mqtt, "Client", lambda *a, **k: DummyClient())
    monkeypatch.setattr(mqtt_handler, "clean_mac", lambda s: "deadbeef")
    monkeypatch.setattr(config, "ID_SUFFIX", "_T", raising=False)
    monkeypatch.setattr(config, "BRIDGE_NAME", "Bridge", raising=False)
    monkeypatch.setattr(config, "BRIDGE_ID", "bridgeid", raising=False)
    monkeypatch.setattr(config, "RTL_EXPIRE_AFTER", 60, raising=False)
    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", False, raising=False)


def _discovery_payload(client: DummyClient, domain: str, unique_id_with_suffix: str) -> dict:
    topic = f"homeassistant/{domain}/{unique_id_with_suffix}/config"
    _t, payload, _r = last_published(client, topic)
    return json.loads(payload)


def test_battery_ok_latches_low_and_clears_after_delay(monkeypatch):
    """battery_ok is published as a binary_sensor where ON means LOW.

    When a device reports low once, we keep it latched low until it has been OK
    continuously for BATTERY_OK_CLEAR_AFTER seconds.
    """
    _patch_common(monkeypatch)

    # Keep it non-diagnostic for this test.
    monkeypatch.setattr(config, "MAIN_SENSORS", ["battery_ok"], raising=False)
    monkeypatch.setattr(config, "BATTERY_OK_CLEAR_AFTER", 10, raising=False)

    # Deterministic time.
    times = iter([0.0, 1.0, 11.0, 12.0])
    monkeypatch.setattr(mqtt_handler.time, "time", lambda: next(times))

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # First low => ON
    h.send_sensor("aa:bb", "battery_ok", 0, "Dev", "NotBridge", is_rtl=False)

    # Migration helper should clear any old numeric *sensor* entity once.
    old_cfg_topic = "homeassistant/sensor/rtl433_NotBridge_deadbeef_battery_ok_T/config"
    clears = [(t, p, r) for (t, p, r) in c.published if t == old_cfg_topic]
    assert len(clears) == 1
    assert clears[0][1] == ""
    assert clears[0][2] is True

    # Discovery is binary_sensor + inverted semantics.
    cfg = _discovery_payload(c, "binary_sensor", "rtl433_NotBridge_deadbeef_battery_ok_T")
    assert cfg.get("device_class") == "battery"
    assert cfg.get("payload_on") == "ON"
    assert cfg.get("payload_off") == "OFF"
    assert cfg.get("expire_after") == 86400  # max(RTL_EXPIRE_AFTER, 86400)

    state_topic = "home/rtl_devices/rtl433_NotBridge_deadbeef/battery_ok"
    _t, payload1, _r = last_published(c, state_topic)
    assert payload1 == "ON"

    # Immediately OK again (< clear_after) => still latched LOW
    h.send_sensor("aa:bb", "battery_ok", 1, "Dev", "NotBridge", is_rtl=False)
    assert h._battery_state["rtl433_NotBridge_deadbeef"]["latched_low"] is True

    # After the clear window has passed, OK should clear the latch => OFF
    h.send_sensor("aa:bb", "battery_ok", 1, "Dev", "NotBridge", is_rtl=False)
    _t, payload2, _r = last_published(c, state_topic)
    assert payload2 == "OFF"

    # Another low should not re-clear the old sensor config (migration runs once)
    h.send_sensor("aa:bb", "battery_ok", 0, "Dev", "NotBridge", is_rtl=False)
    clears2 = [(t, p, r) for (t, p, r) in c.published if t == old_cfg_topic]
    assert len(clears2) == 1