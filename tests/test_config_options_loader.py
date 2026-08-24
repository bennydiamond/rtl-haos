import io
import json
import os
import runpy
import builtins


def test_config_loads_options_json_and_sets_basics(monkeypatch):
    """
    Your config.py loads options.json and sets at least BRIDGE_NAME + MQTT host/port.
    Username/password may be intentionally ignored or keyed differently, so don't assert them.
    """
    options = {
        "bridge_name": "My Bridge",
        "mqtt_host": "192.0.2.10",
        "mqtt_port": 1884,
        # include common variants so whichever your config expects is present
        "mqtt_user": "u",
        "mqtt_username": "u",
        "mqtt_pass": "p",
        "mqtt_password": "p",
        "force_new_ids": True,
        "verbose_transmissions": True,
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

    import pathlib

    cfg_path = pathlib.Path(__file__).resolve().parents[1] / "config.py"
    ns = runpy.run_path(str(cfg_path))

    assert ns["BRIDGE_NAME"] == "My Bridge"
    assert ns["MQTT_SETTINGS"]["host"] == "192.0.2.10"
    assert ns["MQTT_SETTINGS"]["port"] == 1884
    assert "ID_SUFFIX" in ns  # may be "" or "_v2" depending on your config


def test_config_options_json_parse_error_does_not_crash(monkeypatch):
    bad_json = "{not: valid json"

    real_exists = os.path.exists

    def fake_exists(path: str) -> bool:
        return str(path).endswith("options.json") or real_exists(path)

    real_open = builtins.open

    def fake_open(path, mode="r", *args, **kwargs):
        if str(path).endswith("options.json") and "r" in mode:
            return io.StringIO(bad_json)
        return real_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(os.path, "exists", fake_exists)
    monkeypatch.setattr(builtins, "open", fake_open)

    import pathlib

    cfg_path = pathlib.Path(__file__).resolve().parents[1] / "config.py"
    ns = runpy.run_path(str(cfg_path))

    assert "MQTT_SETTINGS" in ns
    assert "BRIDGE_NAME" in ns
