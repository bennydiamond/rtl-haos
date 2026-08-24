import os
import pytest
from config import Settings


def test_config_env_override(monkeypatch):
    """Verifies that Environment Variables override defaults."""
    # Set fake env vars
    monkeypatch.setenv("MQTT_HOST", "192.168.1.99")
    monkeypatch.setenv("BRIDGE_ID", "999")
    monkeypatch.setenv("RTL_THROTTLE_INTERVAL", "120")

    # Reload settings
    # Note: pydantic Settings load at instantiation
    settings = Settings()

    assert settings.mqtt_host == "192.168.1.99"
    assert settings.bridge_id == "999"
    assert settings.rtl_throttle_interval == 120
