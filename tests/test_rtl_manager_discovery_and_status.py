"""Extra coverage for rtl_manager.py.

Focus:
- discover_rtl_devices() output parsing
- radio_status field naming
- rtl_loop() error/status mapping + cleanup

These tests stub subprocess.Popen and time.sleep to avoid long-running loops.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_safe_status_suffix_and_priority():
    import rtl_manager as rm

    assert rm._safe_status_suffix(None) == "0"
    assert rm._safe_status_suffix("") == "0"
    assert rm._safe_status_suffix("   ") == "0"
    assert rm._safe_status_suffix("A B") == "A_B"
    assert rm._safe_status_suffix("a/b:c") == "a_b_c"

    # status_id wins
    assert (
        rm._derive_radio_status_field({"status_id": "slot0", "id": "101", "index": 2, "slot": 9})
        == "radio_status_slot0"
    )
    # then id
    assert rm._derive_radio_status_field({"id": "101", "index": 2, "slot": 9}) == "radio_status_101"
    # then index
    assert rm._derive_radio_status_field({"index": 2, "slot": 9}) == "radio_status_2"
    # then slot
    assert rm._derive_radio_status_field({"slot": 9}) == "radio_status_9"


def test_discover_rtl_devices_parses_serial_and_fallback_index(monkeypatch, capsys):
    import rtl_manager as rm

    def fake_run(cmd, capture_output=None, text=None, timeout=None, **kwargs):
        # cmd looks like: ["rtl_eeprom", "-d", "<index>"]
        idx = int(cmd[-1])
        if idx == 0:
            out = "Found something\nSerial number: 101\n"
            return SimpleNamespace(stdout=out, stderr="", returncode=0)
        if idx == 1:
            out = "EEPROM read ok but no serial line here\n"
            return SimpleNamespace(stdout=out, stderr="", returncode=0)
        # stop scan
        out = "No supported devices\n"
        return SimpleNamespace(stdout=out, stderr="", returncode=1)

    monkeypatch.setattr(rm.subprocess, "run", fake_run)

    devices = rm.discover_rtl_devices()
    assert devices[0]["id"] == "101" and devices[0]["index"] == 0
    assert devices[1]["id"] == "1" and devices[1]["index"] == 1

    # Also cover FileNotFoundError path
    def raise_fnf(*_a, **_k):
        raise FileNotFoundError("rtl_eeprom")

    monkeypatch.setattr(rm.subprocess, "run", raise_fnf)
    devices2 = rm.discover_rtl_devices()
    assert devices2 == []
    _ = capsys.readouterr()


def test_rtl_loop_status_mapping_and_cleanup(monkeypatch):
    import rtl_manager as rm

    # Make IDs deterministic and avoid dependency on utils.clean_mac
    monkeypatch.setattr(rm, "clean_mac", lambda x: str(x))

    # Force dew point to a known value
    monkeypatch.setattr(rm, "calculate_dew_point", lambda *_a, **_k: 42.0)

    # Disable debug dump spam, but still exercise the branch by making it callable.
    monkeypatch.setattr(rm, "_debug_dump_packet", lambda **_k: None)

    # Configure rtl_manager's config flags used inside rtl_loop
    monkeypatch.setattr(rm.config, "DEBUG_RAW_JSON", True, raising=False)
    monkeypatch.setattr(rm.config, "RTL_SHOW_TIMESTAMPS", True, raising=False)
    monkeypatch.setattr(rm.config, "DEVICE_BLACKLIST", ["dead*"], raising=False)
    monkeypatch.setattr(rm.config, "DEVICE_WHITELIST", [], raising=False)

    # Control time() so RTL_SHOW_TIMESTAMPS branch publishes on first JSON
    monkeypatch.setattr(rm.time, "time", lambda: 100.0)

    published = []

    class DummyMQTT:
        def send_sensor(self, sys_id, field, value, *a, **k):
            published.append((sys_id, field, value))

    dispatched = []

    class DummyProcessor:
        def dispatch_reading(self, clean_id, field, value, *a, **k):
            dispatched.append((clean_id, field, value))

    # Dummy process to feed lines
    class DummyStdout:
        def __init__(self, lines):
            self._lines = list(lines)

        def readline(self):
            if not self._lines:
                return ""
            return self._lines.pop(0)

    class DummyProc:
        def __init__(self, lines):
            self.stdout = DummyStdout(lines)
            self._poll = None

        def poll(self):
            # treat as exited once stdout is empty
            if self.stdout._lines:
                return None
            return 1

        def terminate(self):
            return None

        def wait(self, timeout=None):
            return None

        def kill(self):
            return None

    # Lines: ignore noise, then an error mapping, then a blocked device, then a valid device JSON.
    lines = [
        "Detached kernel driver\n",
        "usb_claim_interface: Device or resource busy\n",
        '{"model":"Any","id":"deadbeef","type":"gas","temperature_C":0,"humidity":50}\n',
        '{"model":"Neptune-R900","id":"01","type":"water","consumption":20,"temperature_C":0,"humidity":50}\n',
    ]

    def fake_popen(*_a, **_k):
        return DummyProc(lines)

    monkeypatch.setattr(rm.subprocess, "Popen", fake_popen)

    # Stop rtl_loop after the first outer-while iteration.
    def stop_sleep(_secs):
        raise StopIteration()

    monkeypatch.setattr(rm.time, "sleep", stop_sleep)

    radio = {
        "name": "RTL_101",
        "id": "101",
        "index": 0,
        "freq": "915M,433M",
        "hop_interval": 0,  # should auto-default to 60 when multiple freqs
        "rate": "1024k",
        "protocols": [1, 2],
    }

    with pytest.raises(StopIteration):
        rm.rtl_loop(radio, DummyMQTT(), DummyProcessor(), "sys", "Bridge")

    # Status mapping should have produced a friendly USB busy status
    assert any("Error: USB busy" in v for (_sid, _f, v) in published)

    # Neptune-R900 conversion should have dispatched meter_reading=2.0
    assert ("01", "meter_reading", 2.0) in dispatched

    # Dew point derived publish should have been attempted
    assert any(f == "dew_point" and v == 42.0 for (_cid, f, v) in dispatched)
