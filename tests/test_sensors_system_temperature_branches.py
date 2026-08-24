import types
import time as _time

import sensors_system


def _mk_temp(current):
    return types.SimpleNamespace(current=current)


def test_system_monitor_reads_coretemp_branch(monkeypatch):
    # Avoid slow cpu_percent(interval=1)
    monkeypatch.setattr(sensors_system.psutil, "cpu_percent", lambda interval=1: 0)
    monkeypatch.setattr(
        sensors_system.psutil, "virtual_memory", lambda: types.SimpleNamespace(percent=0)
    )
    monkeypatch.setattr(sensors_system.psutil, "boot_time", lambda: _time.time() - 100)
    monkeypatch.setattr(
        sensors_system.psutil,
        "Process",
        lambda pid: types.SimpleNamespace(memory_info=lambda: types.SimpleNamespace(rss=0)),
    )

    monkeypatch.setattr(
        sensors_system.psutil, "sensors_temperatures", lambda: {"coretemp": [_mk_temp(55.0)]}
    )

    sm = sensors_system.SystemMonitor()
    stats = sm.read_stats()
    assert stats.get("sys_temp") == 55.0


def test_system_monitor_temperature_exception_path(monkeypatch):
    monkeypatch.setattr(sensors_system.psutil, "cpu_percent", lambda interval=1: 0)
    monkeypatch.setattr(
        sensors_system.psutil, "virtual_memory", lambda: types.SimpleNamespace(percent=0)
    )
    monkeypatch.setattr(sensors_system.psutil, "boot_time", lambda: _time.time() - 100)
    monkeypatch.setattr(
        sensors_system.psutil,
        "Process",
        lambda pid: types.SimpleNamespace(memory_info=lambda: types.SimpleNamespace(rss=0)),
    )

    def boom():
        raise RuntimeError("no temps")

    monkeypatch.setattr(sensors_system.psutil, "sensors_temperatures", boom)

    sm = sensors_system.SystemMonitor()
    stats = sm.read_stats()
    assert "sys_temp" not in stats
