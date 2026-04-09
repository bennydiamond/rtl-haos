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


def test_gas_default_ft3_when_configured(monkeypatch):
    """Gas readings should publish in ft³ (raw) and not be scaled."""
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["Consumption"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # Learn it's gas (so _utility_meta_override is exercised)
    h.send_sensor("device_x", "MeterType", "Gas", "SCMplus deadbeef", "SCMplus")
    h.send_sensor("device_x", "Consumption", 12345, "SCMplus deadbeef", "SCMplus")

    cfg = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SCMplus_deadbeef_Consumption_T")
    assert cfg.get("unit_of_measurement") == "ft³"
    assert cfg.get("device_class") == "gas"

    st = last_state_payload(c, "rtl433_SCMplus_deadbeef", "Consumption")
    assert_float_str(st, 12345.0)


def test_water_non_neptune_meter_reading_remains_ft3(monkeypatch):
    """Non-Neptune water meters should keep ft³ when commodity is water."""
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["meter_reading"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # Learn it's water first so _utility_meta_override is exercised for this device.
    h.send_sensor("device_x", "type", "water", "SomeWater deadbeef", "SomeWater")
    h.send_sensor("device_x", "meter_reading", 55.5, "SomeWater deadbeef", "SomeWater")

    cfg = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SomeWater_deadbeef_meter_reading_T")
    assert cfg.get("device_class") == "water"
    assert cfg.get("unit_of_measurement") == "ft³"

    st = last_state_payload(c, "rtl433_SomeWater_deadbeef", "meter_reading")
    assert_float_str(st, 55.5)
