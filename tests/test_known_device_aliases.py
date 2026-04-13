"""Tests for alias mapping and conflict resolution."""

from unittest.mock import Mock

from known_device_aliases import KnownDeviceAliases
from known_device_manager import KnownDeviceManager


def test_alias_binding_conflict_resolution_prefers_highest_matches():
    aliases = KnownDeviceAliases(
        alias_bindings={
            "alias_a": {"device_compound_id": "rtl433_Model_1", "matches": 1},
            "alias_b": {"device_compound_id": "rtl433_Model_1", "matches": 9},
            "alias_a_alt": {"device_compound_id": "rtl433_Model_2", "matches": 7},
        }
    )

    r1 = aliases.resolve("rtl433_Model_1", "Model 1")
    r2 = aliases.resolve("rtl433_Model_2", "Model 2")

    assert r1["compound_id"] == "rtl433_virtual_alias_b"
    assert r2["compound_id"] == "rtl433_virtual_alias_a_alt"


def test_single_alias_and_single_device_enforced():
    aliases = KnownDeviceAliases(
        alias_bindings={
            "patio": {"device_compound_id": "rtl433_Acurite_3398", "matches": 3},
            "garage": {"device_compound_id": "rtl433_Acurite_3398", "matches": 2},
            "patio_dup": {"device_compound_id": "rtl433_Acurite_4471", "matches": 1},
        }
    )

    winner = aliases.resolve("rtl433_Acurite_3398", "Acurite")
    assert winner["compound_id"] == "rtl433_virtual_patio"

    assert "garage" not in aliases._alias_device_resolved

    other = aliases.resolve("rtl433_Acurite_4471", "Acurite")
    assert other["compound_id"] == "rtl433_virtual_patio_dup"


def test_manager_allows_processing_explicit_alias_binding_when_discovery_off():
    store = Mock()
    store.load_devices.return_value = {}
    store.load_alias_bindings.return_value = {
        "patio_sensor": {
            "device_compound_id": "rtl433_Acurite_3398",
            "matches": 5,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda _topics, _name: None,
        mqtt_update_select_callback=None,
    )

    assert manager.should_process_frame("rtl433_Acurite_3398") is True


def test_bind_alias_rebind_removes_previous_device_and_cleans_topics():
    cleanup_calls = []

    store = Mock()
    store.load_devices.return_value = {
        "rtl433_Device_0001": {
            "name": "Temp 0001",
            "topics": [
                "homeassistant/sensor/rtl433_Device_0001_temperature/config",
                "home/rtl_devices/rtl433_Device_0001/temperature",
            ],
        },
        "rtl433_Device_0002": {
            "name": "Temp 0002",
            "topics": [
                "homeassistant/sensor/rtl433_Device_0002_temperature/config",
                "home/rtl_devices/rtl433_Device_0002/temperature",
            ],
        },
    }
    store.load_alias_bindings.return_value = {
        "temp_sensor": {
            "device_compound_id": "rtl433_Device_0001",
            "matches": 1,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda topics, name: cleanup_calls.append((topics, name)),
        mqtt_update_select_callback=None,
    )

    ok = manager.bind_alias_to_device("temp_sensor", "rtl433_Device_0002")
    assert ok is True

    resolved = manager.resolve_device_identity("rtl433_Device_0002", "Temp 0002")
    assert resolved["compound_id"] == "rtl433_virtual_temp_sensor"

    # Preferred behavior: both stale physical identities are removed.
    assert "rtl433_Device_0001" not in manager.known_devices
    assert "rtl433_Device_0002" not in manager.known_devices

    # Rebind cleanup should include old alias target and new target physical topics.
    cleaned_topic_sets = [set(topics) for topics, _name in cleanup_calls]
    assert any("home/rtl_devices/rtl433_Device_0001/temperature" in s for s in cleaned_topic_sets)
    assert any("home/rtl_devices/rtl433_Device_0002/temperature" in s for s in cleaned_topic_sets)

    assert store.save_alias_bindings.called
    assert store.save_devices.called


def test_unbind_alias_persists_and_stops_resolving_to_alias():
    store = Mock()
    store.load_devices.return_value = {}
    store.load_alias_bindings.return_value = {
        "temp_sensor": {
            "device_compound_id": "rtl433_Device_0002",
            "matches": 4,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda _topics, _name: None,
        mqtt_update_select_callback=None,
    )

    assert manager.unbind_alias("temp_sensor") is True
    resolved = manager.resolve_device_identity("rtl433_Device_0002", "Temp 0002")
    assert resolved["compound_id"] == "rtl433_Device_0002"
    assert store.save_alias_bindings.called


def test_delete_alias_and_bound_device_removes_both_and_cleans_topics():
    cleanup_calls = []

    store = Mock()
    store.load_devices.return_value = {
        "rtl433_Device_0002": {
            "name": "Temp 0002",
            "topics": [
                "homeassistant/sensor/rtl433_Device_0002_temperature/config",
                "home/rtl_devices/rtl433_Device_0002/temperature",
            ],
        }
    }
    store.load_alias_bindings.return_value = {
        "temp_sensor": {
            "device_compound_id": "rtl433_Device_0002",
            "matches": 4,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda topics, name: cleanup_calls.append((topics, name)),
        mqtt_update_select_callback=None,
    )

    assert manager.delete_alias_and_bound_device("temp_sensor") is True
    assert "temp_sensor" not in manager.alias_bindings
    assert "rtl433_Device_0002" not in manager.known_devices
    assert store.save_alias_bindings.called
    assert store.save_devices.called

    assert cleanup_calls
    topics, name = cleanup_calls[-1]
    assert "home/rtl_devices/rtl433_Device_0002/temperature" in topics
    assert name == "Temp 0002"


def test_get_removable_device_options_prefers_alias_name_over_bound_ids():
    store = Mock()
    store.load_devices.return_value = {
        "rtl433_virtual_temp_sensor_ab12": {"name": "Temp Sensor", "topics": []},
        "rtl433_Other_1010": {"name": "Other", "topics": []},
    }
    store.load_alias_bindings.return_value = {
        "temp_sensor": {
            "device_compound_id": "rtl433_Device_0002",
            "logical_compound_id": "rtl433_virtual_temp_sensor_ab12",
            "matches": 3,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda _topics, _name: None,
        mqtt_update_select_callback=None,
    )

    opts = manager.get_removable_device_options()
    assert "temp_sensor" in opts
    assert "rtl433_virtual_temp_sensor_ab12" not in opts
    assert "rtl433_Other_1010" in opts


def test_get_known_devices_with_names_shows_alias_for_bound_device():
    store = Mock()
    store.load_devices.return_value = {
        "rtl433_virtual_multisensor_congelateur_vertical": {
            "name": "MultiSensor Congelateur Vertical",
            "topics": [],
        },
        "rtl433_Other_1010": {
            "name": "Other Device",
            "topics": [],
        },
    }
    store.load_alias_bindings.return_value = {
        "cong_lateur_vertical": {
            "device_compound_id": "rtl433_Acurite_4471",
            "logical_compound_id": "rtl433_virtual_multisensor_congelateur_vertical",
            "matches": 3,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda _topics, _name: None,
        mqtt_update_select_callback=None,
    )

    opts = manager.get_known_devices_with_names()
    assert opts.get("cong_lateur_vertical") == "rtl433_Acurite_4471"
    assert "MultiSensor Congelateur Vertical" not in opts
    assert opts.get("Other Device") == "rtl433_Other_1010"


def test_remove_device_with_alias_token_uses_alias_delete_path():
    cleanup_calls = []

    store = Mock()
    store.load_devices.return_value = {
        "rtl433_virtual_temp_sensor_ab12": {
            "name": "Temp Sensor",
            "topics": ["home/rtl_devices/rtl433_virtual_temp_sensor_ab12/temperature"],
        },
        "rtl433_Device_0002": {
            "name": "Temp 0002",
            "topics": ["home/rtl_devices/rtl433_Device_0002/temperature"],
        },
    }
    store.load_alias_bindings.return_value = {
        "temp_sensor": {
            "device_compound_id": "rtl433_Device_0002",
            "logical_compound_id": "rtl433_virtual_temp_sensor_ab12",
            "matches": 3,
        }
    }

    manager = KnownDeviceManager(
        known_device_store=store,
        get_discovery_enabled_callback=lambda: False,
        mqtt_cleanup_callback=lambda topics, name: cleanup_calls.append((topics, name)),
        mqtt_update_select_callback=None,
    )

    manager.remove_device("temp_sensor")

    assert "temp_sensor" not in manager.alias_bindings
    assert "rtl433_virtual_temp_sensor_ab12" not in manager.known_devices
    assert "rtl433_Device_0002" not in manager.known_devices
    assert cleanup_calls
