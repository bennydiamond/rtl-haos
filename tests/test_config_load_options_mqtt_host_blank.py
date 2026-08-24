import json
import os
import importlib

import config


def test_load_options_sets_default_mqtt_host_when_blank(tmp_path, monkeypatch):
    opts = {"mqtt_host": ""}  # blank should set MQTT_HOST default
    p = tmp_path / "options.json"
    p.write_text(json.dumps(opts), encoding="utf-8")

    monkeypatch.setattr(config, "OPTIONS_PATH", str(p), raising=False)
    # Ensure env is clean
    os.environ.pop("MQTT_HOST", None)

    # Call loader directly (covers branch without requiring module re-import)
    config._load_ha_options_into_env()

    assert os.environ.get("MQTT_HOST") == "core-mosquitto"
