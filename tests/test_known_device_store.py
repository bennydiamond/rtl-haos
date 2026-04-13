import json

from known_device_store import KnownDeviceStore


def test_load_devices_accepts_new_devices_map(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "rtl433_ModelA_123": {},
                    "rtl433_ModelB_456": {"last_seen": 1712345678},
                }
            }
        ),
        encoding="utf-8",
    )

    store = KnownDeviceStore(str(path))

    assert store.load_devices() == {
        "rtl433_ModelA_123": {},
        "rtl433_ModelB_456": {"last_seen": 1712345678},
    }


def test_save_devices_writes_devices_map(tmp_path):
    path = tmp_path / "known_devices.json"
    store = KnownDeviceStore(str(path))

    store.save_devices({
        "rtl433_ModelB_456": {},
        "rtl433_ModelA_123": {}
    })

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "devices": {
            "rtl433_ModelA_123": {},
            "rtl433_ModelB_456": {},
        }
    }


def test_save_devices_preserves_existing_metadata(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "rtl433_ModelA_123": {"last_seen": 1712345678},
                    "rtl433_ModelB_456": {"foo": "bar"},
                }
            }
        ),
        encoding="utf-8",
    )
    store = KnownDeviceStore(str(path))

    store.save_devices({
        "rtl433_ModelA_123": {"last_seen": 1712345678},
        "rtl433_ModelC_789": {}
    })

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "devices": {
            "rtl433_ModelA_123": {"last_seen": 1712345678},
            "rtl433_ModelC_789": {},
        }
    }


def test_save_devices_preserves_unrelated_top_level_keys(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "rtl433_ModelA_123": {"name": "Old"},
                },
                "meta": {"version": 1},
            }
        ),
        encoding="utf-8",
    )

    store = KnownDeviceStore(str(path))
    store.save_devices({"rtl433_ModelB_456": {"name": "New"}})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["devices"] == {"rtl433_ModelB_456": {"name": "New"}}
    assert payload["meta"] == {"version": 1}


def test_load_alias_bindings_reads_second_json_set(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {},
                "alias_bindings": {
                    "patio_sensor": {
                        "device_compound_id": "rtl433_Acurite_3398",
                        "matches": 12,
                    },
                    "bad_entry": "not_a_dict",
                },
            }
        ),
        encoding="utf-8",
    )

    store = KnownDeviceStore(str(path))

    assert store.load_alias_bindings() == {
        "patio_sensor": {
            "device_compound_id": "rtl433_Acurite_3398",
            "matches": 12,
        }
    }


def test_save_devices_preserves_alias_bindings(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "rtl433_ModelA_123": {"name": "Old"},
                },
                "alias_bindings": {
                    "patio_sensor": {
                        "device_compound_id": "rtl433_Acurite_3398",
                        "matches": 7,
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    store = KnownDeviceStore(str(path))
    store.save_devices({"rtl433_ModelB_456": {"name": "New"}})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["devices"] == {"rtl433_ModelB_456": {"name": "New"}}
    assert payload["alias_bindings"] == {
        "patio_sensor": {
            "device_compound_id": "rtl433_Acurite_3398",
            "matches": 7,
        }
    }


def test_save_alias_bindings_preserves_devices(tmp_path):
    path = tmp_path / "known_devices.json"
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "rtl433_ModelA_123": {"name": "Device A"},
                },
                "alias_bindings": {},
            }
        ),
        encoding="utf-8",
    )

    store = KnownDeviceStore(str(path))
    store.save_alias_bindings(
        {
            "temp_sensor": {
                "device_compound_id": "rtl433_ModelA_123",
                "matches": 2,
            }
        }
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["devices"] == {"rtl433_ModelA_123": {"name": "Device A"}}
    assert payload["alias_bindings"] == {
        "temp_sensor": {
            "device_compound_id": "rtl433_ModelA_123",
            "matches": 2,
        }
    }