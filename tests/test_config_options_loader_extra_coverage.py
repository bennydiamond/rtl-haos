import builtins
import io
import json
import os
import pathlib
import runpy


def test_config_options_blank_mqtt_host_defaults_to_core_mosquitto(monkeypatch):
    """Covers config._load_ha_options_into_env list/dict handling and mqtt_host blank default."""

    options = {
        "mqtt_host": "",  # intentionally blank in HA UI
        "mqtt_port": 1883,
        # list/dict types should be json.dumps'd into env vars
        "device_blacklist": ["Bad*", "Nope*"],
        "rtl_config": [
            {"name": "RTL_0", "id": "000", "freq": "433.92M"},
        ],
    }
    options_json = json.dumps(options)

    real_exists = os.path.exists

    def fake_exists(path: str) -> bool:
        return str(path).endswith("options.json") or real_exists(path)

    real_open = builtins.open

    def fake_open(path, mode="r", *args, **kwargs):
        if str(path).endswith("options.json") and "r" in mode:
            return io.StringIO(options_json)
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(os.path, "exists", fake_exists)
    monkeypatch.setattr(builtins, "open", fake_open)

    monkeypatch.delenv("MQTT_HOST", raising=False)
    monkeypatch.delenv("DEVICE_BLACKLIST", raising=False)
    monkeypatch.delenv("RTL_CONFIG", raising=False)

    cfg_path = pathlib.Path(__file__).resolve().parents[1] / "config.py"
    ns = runpy.run_path(str(cfg_path))

    assert ns["MQTT_SETTINGS"]["host"] == "core-mosquitto"
    assert ns["DEVICE_BLACKLIST"] == ["Bad*", "Nope*"]
    assert isinstance(ns["RTL_CONFIG"], list)
    assert ns["RTL_CONFIG"][0]["freq"] == "433.92M"
