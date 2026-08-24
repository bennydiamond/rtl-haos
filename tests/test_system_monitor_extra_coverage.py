import inspect
import pytest


def test_format_list_for_ha_empty_and_nonempty():
    import system_monitor

    assert system_monitor.format_list_for_ha([]) == "None"
    assert system_monitor.format_list_for_ha(["a", "b"]) == "a, b"


def _run_one_iteration(system_monitor, mqtt):
    """
    Stop the loop after the first sleep using either sleep_fn (if supported)
    or by patching time.sleep.
    """
    calls = {"n": 0}

    def stop_sleep(_s):
        calls["n"] += 1
        raise KeyboardInterrupt()

    sig = inspect.signature(system_monitor.system_stats_loop)
    kwargs = {}

    if "interval" in sig.parameters:
        kwargs["interval"] = 0
    if "sleep_fn" in sig.parameters:
        kwargs["sleep_fn"] = stop_sleep
    if "max_iterations" in sig.parameters:
        kwargs["max_iterations"] = 1

    if "sleep_fn" not in kwargs:
        # fallback: patch time.sleep inside module
        system_monitor.time.sleep = stop_sleep

    with pytest.raises(KeyboardInterrupt):
        system_monitor.system_stats_loop(mqtt, "sysid", "Bridge", **kwargs)


def test_system_stats_loop_psutil_success_path(monkeypatch):
    import system_monitor

    sent = []

    class DummyMQTT:
        tracked_devices = {"A", "B", "C"}

        def send_sensor(self, *a, **k):
            sent.append((a, k))

    monkeypatch.setattr(system_monitor, "PSUTIL_AVAILABLE", True)

    class DummyMon:
        def read_stats(self):
            return {
                "cpu_percent": 1.0,
                "mem_percent": 2.0,
                "disk_percent": 3.0,
                "host_ip": "127.0.0.1",
            }

    monkeypatch.setattr(system_monitor, "SystemMonitor", lambda: DummyMon())

    _run_one_iteration(system_monitor, DummyMQTT())
    assert sent, "Expected at least one send_sensor call"


def test_system_stats_loop_handles_systemmonitor_init_failure(monkeypatch, capsys):
    import system_monitor

    class DummyMQTT:
        tracked_devices = set()

        def send_sensor(self, *a, **k):
            return

    monkeypatch.setattr(system_monitor, "PSUTIL_AVAILABLE", True)

    def boom():
        raise RuntimeError("no psutil access")

    monkeypatch.setattr(system_monitor, "SystemMonitor", boom)

    _run_one_iteration(system_monitor, DummyMQTT())

    out = capsys.readouterr().out.lower()
    assert "hardware monitor failed to start" in out
