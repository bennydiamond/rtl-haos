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


def _count_publishes(client: DummyClient, topic: str) -> int:
    return sum(1 for (t, _p, _r) in client.published if t == topic)


def test_consumption_data_updates_to_kwh_after_ert_type(monkeypatch):
    """
    If a generic utility field arrives before we know commodity (e.g. ERT-SCM),
    it may be discovered with default metadata. When ert_type arrives later and
    indicates 'electric', we should refresh discovery + re-publish state using
    energy/kWh metadata.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["consumption_data"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # 1) consumption_data arrives first (before ert_type commodity hint)
    h.send_sensor("device_x", "consumption_data", 217504, "ERT-SCM deadbeef", "ERT-SCM")

    cfg1 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_ERT-SCM_deadbeef_consumption_data_T")
    assert cfg1.get("device_class") == "gas"
    assert cfg1.get("unit_of_measurement") == "ft³"

    st1 = last_state_payload(c, "rtl433_ERT-SCM_deadbeef", "consumption_data")
    assert_float_str(st1, 217504.0)

    state_topic = "home/rtl_devices/rtl433_ERT-SCM_deadbeef/consumption_data"
    state_count_1 = _count_publishes(c, state_topic)

    # 2) ert_type arrives later indicating electric (4 is in the electric set)
    h.send_sensor("device_x", "ert_type", 4, "ERT-SCM deadbeef", "ERT-SCM")

    cfg2 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_ERT-SCM_deadbeef_consumption_data_T")
    assert cfg2.get("device_class") == "energy"
    assert cfg2.get("unit_of_measurement") == "kWh"

    # Discovery metadata changed -> we should also re-publish the cached state.
    st2 = last_state_payload(c, "rtl433_ERT-SCM_deadbeef", "consumption_data")
    assert_float_str(st2, 2175.04)

    state_count_2 = _count_publishes(c, state_topic)
    assert state_count_2 == state_count_1 + 1


def test_meter_type_does_not_require_gas_volume_unit(monkeypatch):
    """
    Regression: removing gas_volume_unit must not break late MeterType refresh paths.
    We don't expect any ft³->CCF conversion anymore.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["Consumption"], raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # Consumption first
    h.send_sensor("device_x", "Consumption", 12345, "SCMplus deadbeef", "SCMplus")
    cfg1 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SCMplus_deadbeef_Consumption_T")
    assert cfg1.get("device_class") == "gas"
    assert cfg1.get("unit_of_measurement") == "ft³"

    # MeterType later (no conversion expected; just ensure no crash and still ft³)
    h.send_sensor("device_x", "MeterType", "Gas", "SCMplus deadbeef", "SCMplus")
    cfg2 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SCMplus_deadbeef_Consumption_T")
    assert cfg2.get("device_class") == "gas"
    assert cfg2.get("unit_of_measurement") == "ft³"

    st = last_state_payload(c, "rtl433_SCMplus_deadbeef", "Consumption")
    assert_float_str(st, 12345.0)


def test_gas_unit_ccf_converts_and_refreshes_after_meter_type(monkeypatch):
    """If gas_unit=ccf, gas totals should publish as CCF (ft³ ÷ 100).

    This also validates the "late commodity" refresh path: if the main total
    arrives before MeterType, we should re-publish discovery + state once the
    commodity becomes known, applying the user's gas_unit preference.
    """
    _patch_common(monkeypatch)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["Consumption"], raising=False)
    monkeypatch.setattr(config.settings, "gas_unit", "ccf", raising=False)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    c = h.client

    # 1) Total arrives first (commodity unknown) -> default gas ft³ metadata.
    h.send_sensor("device_x", "Consumption", 12345, "SCMplus deadbeef", "SCMplus")
    cfg1 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SCMplus_deadbeef_Consumption_T")
    assert cfg1.get("device_class") == "gas"
    assert cfg1.get("unit_of_measurement") == "ft³"
    st1 = last_state_payload(c, "rtl433_SCMplus_deadbeef", "Consumption")
    assert_float_str(st1, 12345.0)

    state_topic = "home/rtl_devices/rtl433_SCMplus_deadbeef/Consumption"
    state_count_1 = _count_publishes(c, state_topic)

    # 2) MeterType arrives later -> commodity becomes gas; refresh should update
    #    discovery to CCF and re-publish the cached total scaled by ÷100.
    h.send_sensor("device_x", "MeterType", "Gas", "SCMplus deadbeef", "SCMplus")

    cfg2 = last_discovery_payload(c, domain="sensor", unique_id_with_suffix="rtl433_SCMplus_deadbeef_Consumption_T")
    assert cfg2.get("device_class") == "gas"
    assert cfg2.get("unit_of_measurement") == "CCF"

    st2 = last_state_payload(c, "rtl433_SCMplus_deadbeef", "Consumption")
    assert_float_str(st2, 123.45)

    state_count_2 = _count_publishes(c, state_topic)
    assert state_count_2 == state_count_1 + 1
