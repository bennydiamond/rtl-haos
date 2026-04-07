import builtins
import importlib
import sys


def _reload_system_monitor(monkeypatch, *, find_spec_behavior, import_psutil_error: bool):
    """Reload system_monitor with controlled psutil detection/import behavior."""
    # Ensure a clean import so module-level PSUTIL_AVAILABLE code runs again.
    sys.modules.pop("system_monitor", None)

    # Patch find_spec used inside system_monitor.
    monkeypatch.setattr(importlib.util, "find_spec", find_spec_behavior)

    if import_psutil_error:
        orig_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "psutil":
                raise ImportError("psutil blocked for test")
            return orig_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", fake_import)

    import system_monitor  # noqa: F401

    return sys.modules["system_monitor"]


def test_psutil_find_spec_valueerror_disables_psutil(monkeypatch):
    def boom(_name):
        raise ValueError("bad spec")

    sm = _reload_system_monitor(monkeypatch, find_spec_behavior=boom, import_psutil_error=False)
    assert sm.PSUTIL_AVAILABLE is False


def test_psutil_importerror_disables_psutil(monkeypatch):
    sm = _reload_system_monitor(
        monkeypatch,
        find_spec_behavior=lambda _name: object(),
        import_psutil_error=True,
    )
    assert sm.PSUTIL_AVAILABLE is False


def test_format_list_for_ha_and_loop_one_iteration(monkeypatch):
    import system_monitor as sm

    assert sm.format_list_for_ha([]) == "None"

    long_list = ["x" * 10] * 100
    out = sm.format_list_for_ha(long_list)
    assert len(out) <= 250

    # Run a single loop iteration by forcing time.sleep to abort.
    class DummyMQTT:
        def __init__(self):
            self.tracked_devices = {"a", "b"}
            self.calls = []

        def send_sensor(self, device_id, field, value, device_name, model_name, is_rtl=True):
            self.calls.append((device_id, field, value, device_name, model_name, is_rtl))

    dm = DummyMQTT()
    monkeypatch.setattr(sm, "PSUTIL_AVAILABLE", False)
    monkeypatch.setattr(sm, "get_rtl_433_version_cached", lambda: "rtl_433 version test")

    def stop(_sec):
        raise StopIteration

    monkeypatch.setattr(sm.time, "sleep", stop)

    try:
        sm.system_stats_loop(dm, "dev", "Model")
    except StopIteration:
        pass

    assert any(field == "sys_rtl_433_version" and value == "rtl_433 version test" for (_d, field, value, *_rest) in dm.calls)
