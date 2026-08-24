import builtins
import importlib
import sys
import types
import pytest


_ORIG_PRINT = builtins.print


def import_main_safely():
    sys.modules.pop("main", None)
    builtins.print = _ORIG_PRINT
    m = importlib.import_module("main")
    builtins.print = _ORIG_PRINT
    return m


def test_get_source_color_supported_unsupported():
    m = import_main_safely()
    assert m.get_source_color("UNSUPPORTED device") == m.c_yellow
    assert m.get_source_color("SUPPORTED device") == m.c_green


def test_timestamped_print_error_warning_and_tx_branches(monkeypatch):
    m = import_main_safely()

    captured = []

    def cap(*args, **kwargs):
        captured.append(args[0] if args else "")

    monkeypatch.setattr(m, "_original_print", cap)

    # Freeze time
    class FakeDT:
        @staticmethod
        def now():
            return types.SimpleNamespace(strftime=lambda fmt: "00:00:00")

    monkeypatch.setattr(m, "datetime", FakeDT)

    # ERROR path
    m.timestamped_print("ERROR: something bad happened")
    # WARN path
    m.timestamped_print("WARNING: beware")
    # TX path with formatter match
    m.timestamped_print("[RTL] -> TX [SRC]: 123")

    joined = "\n".join(captured)
    assert "ERROR" in joined
    assert "WARN" in joined
    assert "DATA" in joined


def test_check_dependencies_paho_missing_exits(monkeypatch):
    m = import_main_safely()

    # rtl_433 exists
    monkeypatch.setattr(
        m.subprocess, "run", lambda *a, **k: types.SimpleNamespace(stdout=b"/usr/bin/rtl_433\n")
    )
    # paho missing
    monkeypatch.setattr(m.importlib.util, "find_spec", lambda name: None)

    with pytest.raises(SystemExit):
        m.check_dependencies()
