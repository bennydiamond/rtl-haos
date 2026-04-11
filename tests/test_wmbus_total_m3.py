import config
import mqtt_handler

from ._mqtt_test_helpers import DummyClient, assert_float_str, last_discovery_payload, last_state_payload


def _patch_common(monkeypatch):
    monkeypatch.setattr(mqtt_handler.mqtt, "Client", lambda *a, **k: DummyClient())
    monkeypatch.setattr(mqtt_handler, "clean_mac", lambda s: "deadbeef")
    monkeypatch.setattr(config, "ID_SUFFIX", "_T", raising=False)
    monkeypatch.setattr(config, "BRIDGE_NAME", "Bridge", raising=False)
    monkeypatch.setattr(config, "BRIDGE_ID", "bridgeid", raising=False)
    monkeypatch.setattr(config, "RTL_EXPIRE_AFTER", 60, raising=False)
    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", False, raising=False)


def test_wmbus_total_m3_publishes_as_water_m3(monkeypatch):
    """Wireless M-Bus meters often expose totals as total_m3; ensure we publish correct unit/class."""
    _patch_common(monkeypatch)

    # Ensure this isn't treated as a "main" sensor via config list; device_class should still clear diagnostic.
    monkeypatch.setattr(config, "MAIN_SENSORS", [], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    h.send_sensor("device_x", "total_m3", 161.963, "EquaScan deadbeef", "EquaScan")

    cfg = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_EquaScan_deadbeef_total_m3_T")
    assert cfg.get("device_class") == "water"
    assert cfg.get("unit_of_measurement") == "m³"
    assert "entity_category" not in cfg

    st = last_state_payload(c, "rtl433_EquaScan_deadbeef", "total_m3")
    assert_float_str(st, 161.963)
