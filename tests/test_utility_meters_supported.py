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


def test_badger_orion_volume_gal_discovery(monkeypatch):
    """Badger-ORION emits volume_gal; ensure it publishes as a water total."""
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["volume_gal"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    h.send_sensor("device_x", "volume_gal", 12345, "Badger deadbeef", "Badger-ORION")

    cfg = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_Badger-ORION_deadbeef_volume_gal_T")
    assert cfg.get("device_class") == "water"
    assert cfg.get("unit_of_measurement") == "gal"
    assert cfg.get("state_class") == "total_increasing"
    assert "entity_category" not in cfg  # not diagnostic

    st = last_state_payload(c, "rtl433_Badger-ORION_deadbeef", "volume_gal")
    assert_float_str(st, 12345.0)


def test_neptune_r900_meter_reading_uses_gallons(monkeypatch):
    """Neptune-R900 meter_reading should be published in gallons (model-aware)."""
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["meter_reading"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # 1) Reading arrives first (no commodity hint yet)
    h.send_sensor("device_x", "meter_reading", 100.0, "Neptune deadbeef", "Neptune-R900")
    cfg1 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_Neptune-R900_deadbeef_meter_reading_T")
    assert cfg1.get("unit_of_measurement") == "gal"
    assert cfg1.get("device_class") == "water"

    # 2) Type arrives later; triggers refresh but should remain gallons
    h.send_sensor("device_x", "type", "water", "Neptune deadbeef", "Neptune-R900")
    cfg2 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_Neptune-R900_deadbeef_meter_reading_T")
    assert cfg2.get("unit_of_measurement") == "gal"
    assert cfg2.get("device_class") == "water"


def test_ert_scm_electric_inference_updates_discovery(monkeypatch):
    """ERT-SCM classification can arrive late; ensure discovery updates to energy/kWh."""
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["consumption_data"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # 1) consumption_data arrives first
    # UPDATE: We no longer auto-scale by 0.01. We report the RAW value (2735618).
    h.send_sensor("device_x", "consumption_data", 2735618, "ERT deadbeef", "ERT-SCM")
    
    cfg1 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_ERT-SCM_deadbeef_consumption_data_T")
    assert cfg1.get("unit_of_measurement") == "ft³"  # default before ert_type
    
    st1 = last_state_payload(c, "rtl433_ERT-SCM_deadbeef", "consumption_data")
    
    # OLD: assert_float_str(st1, 27356.18)
    # NEW: Expect raw value
    assert_float_str(st1, 2735618.0)

    # 2) ert_type arrives (7 = electric per rtlamr conventions)
    h.send_sensor("device_x", "ert_type", 7, "ERT deadbeef", "ERT-SCM")
    cfg2 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_ERT-SCM_deadbeef_consumption_data_T")
    assert cfg2.get("device_class") == "energy"
    assert cfg2.get("unit_of_measurement") == "kWh"