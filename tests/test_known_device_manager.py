"""Tests for KnownDeviceManager."""
import pytest
from unittest.mock import Mock

from known_device_manager import KnownDeviceManager


@pytest.fixture
def mock_store():
    store = Mock()
    store.load_devices.return_value = {"rtl433_ModelA_123": {"name": "ModelA 123", "topics": ["t1"]}}
    return store


@pytest.fixture
def mock_get_discovery():
    return Mock(return_value=True)


@pytest.fixture
def mock_mqtt_cleanup():
    return Mock()


@pytest.fixture
def manager(mock_store, mock_get_discovery, mock_mqtt_cleanup):
    return KnownDeviceManager(
        known_device_store=mock_store,
        get_discovery_enabled_callback=mock_get_discovery,
        mqtt_cleanup_callback=mock_mqtt_cleanup,
    )


def test_init_loads_devices_from_store(manager, mock_store):
    mock_store.load_devices.assert_called_once()
    assert manager.get_known_devices() == {"rtl433_ModelA_123"}


def test_get_discovery_enabled_calls_callback(manager, mock_get_discovery):
    mock_get_discovery.return_value = False
    assert manager.get_discovery_enabled() is False
    mock_get_discovery.assert_called_once()


def test_get_discovery_enabled_handles_exception(manager, mock_get_discovery, capsys):
    mock_get_discovery.side_effect = RuntimeError("MQTT down")
    assert manager.get_discovery_enabled() is False
    out = capsys.readouterr().out
    assert "Error querying discovery state" in out


def test_should_process_frame_allows_all_when_discovery_enabled(manager, mock_get_discovery):
    mock_get_discovery.return_value = True
    assert manager.should_process_frame("rtl433_ModelA_123") is True
    assert manager.should_process_frame("rtl433_ModelB_456") is True


def test_should_process_frame_blocks_unknown_when_discovery_disabled(manager, mock_get_discovery):
    mock_get_discovery.return_value = False
    assert manager.should_process_frame("rtl433_ModelA_123") is True
    assert manager.should_process_frame("rtl433_ModelB_456") is False


def test_add_or_update_device_adds_and_saves(manager, mock_store):
    manager.add_or_update_device("rtl433_ModelB_456", "ModelB 456", ["t2"])

    assert "rtl433_ModelB_456" in manager.get_known_devices()
    expected_devices = {
        "rtl433_ModelA_123": {"name": "ModelA 123", "topics": ["t1"]},
        "rtl433_ModelB_456": {"name": "ModelB 456", "topics": ["t2"]},
    }
    mock_store.save_devices.assert_called_once_with(expected_devices)


def test_add_or_update_device_merges_new_topics_to_existing_device(manager, mock_store):
    manager.add_or_update_device("rtl433_ModelA_123", "ModelA 123", ["t2"])

    expected_devices = {
        "rtl433_ModelA_123": {"name": "ModelA 123", "topics": ["t1", "t2"]},
    }
    mock_store.save_devices.assert_called_once_with(expected_devices)


def test_add_or_update_device_ignores_existing(manager, mock_store):
    manager.add_or_update_device("rtl433_ModelA_123", "ModelA 123", ["t1"])

    assert len(manager.get_known_devices()) == 1
    mock_store.save_devices.assert_not_called()


def test_add_or_update_device_handles_save_error(manager, mock_store, capsys):
    mock_store.save_devices.side_effect = IOError("Disk full")

    manager.add_or_update_device("rtl433_ModelB_456", "ModelB 456", ["t2"])

    assert "rtl433_ModelB_456" in manager.get_known_devices()
    out = capsys.readouterr().out
    assert "Error persisting discovery" in out


def test_remove_device_removes_saves_and_cleans_up_mqtt(manager, mock_store, mock_mqtt_cleanup):
    manager.remove_device("rtl433_ModelA_123")

    assert "rtl433_ModelA_123" not in manager.get_known_devices()
    mock_store.save_devices.assert_called_once_with({})
    mock_mqtt_cleanup.assert_called_once_with(["t1"], "ModelA 123")


def test_remove_device_ignores_unknown(manager, mock_store, mock_mqtt_cleanup):
    manager.remove_device("rtl433_ModelB_456")

    mock_store.save_devices.assert_not_called()
    mock_mqtt_cleanup.assert_not_called()


def test_remove_device_handles_invalid_format(manager, mock_store, mock_mqtt_cleanup):
    manager.remove_device("invalid_format")

    mock_store.save_devices.assert_not_called()
    mock_mqtt_cleanup.assert_not_called()


def test_clear_all_devices_removes_all_and_saves(manager, mock_store):
    manager.clear_all_devices()

    assert manager.get_known_devices() == set()
    mock_store.save_devices.assert_called_once_with({})


def test_get_all_known_devices_snapshot_returns_copy(manager):
    snapshot = manager.get_all_known_devices_snapshot()
    assert snapshot == {"rtl433_ModelA_123"}

    snapshot.add("rtl433_ModelB_456")
    assert manager.get_known_devices() == {"rtl433_ModelA_123"}