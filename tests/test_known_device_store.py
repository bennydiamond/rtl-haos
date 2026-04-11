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