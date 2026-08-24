import builtins
import io
import json

import config
import utils


def test_get_system_mac_prefers_bridge_id(monkeypatch):
    utils._SYSTEM_MAC = None
    monkeypatch.setattr(config, "BRIDGE_ID", "STATIC_ID", raising=False)
    assert utils.get_system_mac() == "STATIC_ID"


def test_get_system_mac_falls_back_to_default_hostname(monkeypatch):
    utils._SYSTEM_MAC = None
    monkeypatch.setattr(config, "BRIDGE_ID", "", raising=False)
    monkeypatch.setattr(utils.socket, "gethostname", lambda: "")
    assert utils.get_system_mac() == "rtl-bridge-default"


def test_get_system_mac_handles_hostname_errors(monkeypatch):
    utils._SYSTEM_MAC = None
    monkeypatch.setattr(config, "BRIDGE_ID", "", raising=False)

    def boom():
        raise RuntimeError("no hostname")

    monkeypatch.setattr(utils.socket, "gethostname", boom)
    assert utils.get_system_mac() == "rtl-bridge-error-id"


def test_get_homeassistant_country_code_env_override(monkeypatch):
    monkeypatch.setenv("HOMEASSISTANT_COUNTRY", "us")
    assert utils.get_homeassistant_country_code() == "US"


def test_get_homeassistant_country_code_storage_file(monkeypatch):
    # Ensure env is clear so it tries to read storage.
    monkeypatch.delenv("HOMEASSISTANT_COUNTRY", raising=False)
    monkeypatch.delenv("HA_COUNTRY", raising=False)
    monkeypatch.delenv("COUNTRY", raising=False)

    fake = {"data": {"country": "de"}}
    content = io.StringIO(json.dumps(fake))

    def fake_open(path, *a, **k):
        assert path == "/config/.storage/core.config"
        return content

    monkeypatch.setattr(builtins, "open", fake_open)
    assert utils.get_homeassistant_country_code() == "DE"


def test_validate_radio_config_warns_on_common_mistakes():
    warnings = utils.validate_radio_config({"freq": "433.92", "rate": "250", "hop_interval": 10})
    # Missing 'M' for frequency
    assert any("Did you mean '433.92M'" in w for w in warnings)
    # Hopping with only one frequency
    assert any("Hopping will be ignored" in w for w in warnings)
    # Missing suffix for rate
    assert any("Did you mean '250k'" in w for w in warnings)
    # Missing ID
    assert any("missing a device 'id'" in w for w in warnings)


def test_validate_radio_config_accepts_suffixed_values():
    warnings = utils.validate_radio_config({"freq": "433.92M", "rate": "250k", "id": "0"})
    assert warnings == []


def test_choose_secondary_band_defaults_auto_and_custom():
    assert utils.choose_secondary_band_defaults(plan="auto", country_code="DE") == ("868M", 0)
    assert utils.choose_secondary_band_defaults(plan="auto", country_code="US") == ("915M", 0)
    assert utils.choose_secondary_band_defaults(plan="auto", country_code=None) == ("868M,915M", 15)
    assert utils.choose_secondary_band_defaults(plan="world") == ("868M,915M", 15)

    # Custom override uses hopping when multiple freqs are provided
    assert utils.choose_secondary_band_defaults(plan="custom", secondary_override="868M,915M") == (
        "868M,915M",
        15,
    )

    # Unknown plan is treated as a raw frequency string
    assert utils.choose_secondary_band_defaults(plan="920M") == ("920M", 0)


def test_choose_hopper_band_defaults_avoids_used_freqs():
    # EU gets EU candidates, but filters out overlaps
    freqs = utils.choose_hopper_band_defaults(country_code="DE", used_freqs={"915m", "869.525m"})
    assert "915M" not in freqs
    assert "869.525M" not in freqs
