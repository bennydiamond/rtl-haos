"""Tests for rtl_433 version diagnostics in system_monitor.

These are unit tests:
- No real rtl_433 binary is executed (subprocess.run is patched)
- The system_stats_loop is stopped after one iteration by raising KeyboardInterrupt
  from the patched sleep function.
"""

from __future__ import annotations

import types

import pytest


def test_get_rtl_433_version_cached_calls_subprocess_once(monkeypatch):
    import system_monitor

    # Reset cache for test isolation
    monkeypatch.setattr(system_monitor, "_RTL_433_VERSION_CACHE", None, raising=False)

    calls = {"n": 0}

    def fake_run(*args, **kwargs):
        calls["n"] += 1
        return types.SimpleNamespace(stdout="rtl_433 version 24.01", stderr="")

    monkeypatch.setattr(system_monitor.subprocess, "run", fake_run)

    v1 = system_monitor.get_rtl_433_version_cached()
    v2 = system_monitor.get_rtl_433_version_cached()

    assert v1 == v2
    assert "rtl_433" in v1.lower()
    assert calls["n"] == 1, "Expected subprocess.run to be called once due to caching"


def test_get_rtl_433_version_handles_filenotfound(monkeypatch):
    import system_monitor

    # Reset cache
    monkeypatch.setattr(system_monitor, "_RTL_433_VERSION_CACHE", None, raising=False)

    def fake_run(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr(system_monitor.subprocess, "run", fake_run)

    assert system_monitor.get_rtl_433_version_cached() == "rtl_433 not found"


def test_system_stats_loop_publishes_rtl_433_version_once(monkeypatch):
    import system_monitor

    sent = []

    class DummyMQTT:
        tracked_devices = {"A", "B"}

        def send_sensor(self, *args, **kwargs):
            # args: (DEVICE_ID, key, value, device_name, model_name, ...)
            sent.append((args, kwargs))

    # Avoid psutil path entirely for this unit test
    monkeypatch.setattr(system_monitor, "PSUTIL_AVAILABLE", False, raising=False)

    # Make version deterministic
    monkeypatch.setattr(
        system_monitor, "get_rtl_433_version_cached", lambda: "rtl_433 version 24.01"
    )

    def stop_sleep(_s):
        raise KeyboardInterrupt()

    # Stop after first iteration
    monkeypatch.setattr(system_monitor.time, "sleep", stop_sleep)

    with pytest.raises(KeyboardInterrupt):
        system_monitor.system_stats_loop(DummyMQTT(), "dev1", "rtl-haos-bridge")

    # Ensure the version diagnostic was published
    version_msgs = [m for m in sent if len(m[0]) >= 3 and m[0][1] == "sys_rtl_433_version"]
    assert version_msgs, f"Expected sys_rtl_433_version message, got: {sent}"
    # Only one publication per loop iteration (cached outside the loop)
    assert len(version_msgs) == 1
    assert version_msgs[0][0][2] == "rtl_433 version 24.01"
