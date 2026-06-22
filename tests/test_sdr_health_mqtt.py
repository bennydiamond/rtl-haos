# tests/test_sdr_health_mqtt.py
"""Tests for SDR health alert MQTT publishing."""
import json

import pytest

import config
import mqtt_handler

from ._mqtt_test_helpers import DummyClient, last_published


def _patch_common(monkeypatch):
    """Common patches for MQTT handler tests."""
    monkeypatch.setattr(mqtt_handler.mqtt, "Client", lambda *a, **k: DummyClient())
    monkeypatch.setattr(mqtt_handler, "clean_mac", lambda s: "testdevice")
    monkeypatch.setattr(mqtt_handler, "get_system_mac", lambda: "aa:bb:cc:dd:ee:ff")
    monkeypatch.setattr(config, "ID_SUFFIX", "_T", raising=False)
    monkeypatch.setattr(config, "BRIDGE_NAME", "TestBridge", raising=False)
    monkeypatch.setattr(config, "BRIDGE_ID", "testid", raising=False)
    monkeypatch.setattr(config, "RTL_EXPIRE_AFTER", 60, raising=False)
    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", False, raising=False)


class TestSendHealthAlert:
    """Tests for the send_health_alert method."""

    def test_publishes_binary_sensor_discovery(self, monkeypatch):
        """send_health_alert publishes binary_sensor discovery config."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")

        # Find the discovery config
        config_topic = "homeassistant/binary_sensor/testdevice_sdr_health_alert_T/config"
        found = [p for p in c.published if p[0] == config_topic]
        assert len(found) == 1

        payload = json.loads(found[0][1])
        assert payload["name"] == "SDR Health Alert"
        assert payload["device_class"] == "problem"
        assert payload["payload_on"] == "ON"
        assert payload["payload_off"] == "OFF"
        assert "json_attributes_topic" in payload

    def test_publishes_state_off_when_healthy(self, monkeypatch):
        """send_health_alert publishes OFF state when healthy."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")

        state_topic = "home/rtl_devices/testdevice/sdr_health_alert"
        _t, payload, _r = last_published(c, state_topic)
        assert payload == "OFF"

    def test_publishes_state_on_when_problem(self, monkeypatch):
        """send_health_alert publishes ON state when problem detected."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", True, "USB disconnected", "Test Bridge", "TestBridge")

        state_topic = "home/rtl_devices/testdevice/sdr_health_alert"
        _t, payload, _r = last_published(c, state_topic)
        assert payload == "ON"

    def test_publishes_attributes_with_reason(self, monkeypatch):
        """send_health_alert publishes attributes with reason."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", True, "Restart loop detected", "Test Bridge", "TestBridge")

        attr_topic = "home/rtl_devices/testdevice/sdr_health_alert/attributes"
        _t, payload, _r = last_published(c, attr_topic)
        attrs = json.loads(payload)
        assert attrs["reason"] == "Restart loop detected"

    def test_attributes_show_ok_when_healthy(self, monkeypatch):
        """Attributes show 'OK' when healthy."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")

        attr_topic = "home/rtl_devices/testdevice/sdr_health_alert/attributes"
        _t, payload, _r = last_published(c, attr_topic)
        attrs = json.loads(payload)
        assert attrs["reason"] == "OK"

    def test_discovery_published_once(self, monkeypatch):
        """Discovery is only published once."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")
        h.send_health_alert("testid", True, "Error", "Test Bridge", "TestBridge")
        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")

        config_topic = "homeassistant/binary_sensor/testdevice_sdr_health_alert_T/config"
        found = [p for p in c.published if p[0] == config_topic]
        assert len(found) == 1

    def test_state_transition_publishes(self, monkeypatch):
        """State transitions are published."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        state_topic = "home/rtl_devices/testdevice/sdr_health_alert"

        # Initial healthy
        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")
        states = [p for p in c.published if p[0] == state_topic]
        assert states[-1][1] == "OFF"

        # Transition to problem
        h.send_health_alert("testid", True, "Error", "Test Bridge", "TestBridge")
        states = [p for p in c.published if p[0] == state_topic]
        assert states[-1][1] == "ON"

        # Back to healthy
        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")
        states = [p for p in c.published if p[0] == state_topic]
        assert states[-1][1] == "OFF"

    def test_device_info_in_discovery(self, monkeypatch):
        """Discovery payload includes correct device info."""
        _patch_common(monkeypatch)

        h = mqtt_handler.HomeNodeMQTT(version="vtest")
        c = h.client

        h.send_health_alert("testid", False, "", "Test Bridge", "TestBridge")

        config_topic = "homeassistant/binary_sensor/testdevice_sdr_health_alert_T/config"
        _t, payload, _r = last_published(c, config_topic)
        cfg = json.loads(payload)

        assert "device" in cfg
        assert cfg["device"]["manufacturer"] == "rtl-haos"
        assert cfg["device"]["model"] == "TestBridge"
        assert cfg["device"]["sw_version"] == "vtest"
