import builtins
import types

import rtl_manager


def test_trigger_radio_restart_terminates_running_processes(monkeypatch):
    class P:
        def __init__(self, running=True):
            self._running = running
            self.terminated = False

        def poll(self):
            return None if self._running else 0

        def terminate(self):
            self.terminated = True

    p = P(running=True)
    rtl_manager.ACTIVE_PROCESSES.append(p)
    rtl_manager.trigger_radio_restart()
    assert p.terminated is True

    # Cleanup
    rtl_manager.ACTIVE_PROCESSES.remove(p)


def test_flatten_handles_lists_and_dicts():
    out = rtl_manager.flatten({"a": {"b": 1}, "c": [2, {"d": 3}]})
    assert out["a_b"] == 1
    assert out["c_0"] == 2
    assert out["c_1_d"] == 3


def test_debug_dump_packet_import_field_meta_failure(monkeypatch, capsys):
    # Force "from field_meta import FIELD_META" to fail inside rtl_manager._debug_dump_packet
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "field_meta":
            raise ImportError("boom")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    rtl_manager._debug_dump_packet(
        raw_line='{"time":"t","model":"X","id":1}',
        data_raw={"time": "t", "model": "X", "id": 1},
        data_processed={"time": "t", "model": "X", "id": 1},
        radio_name="R",
        radio_freq="915M",
        model="X",
        clean_id="1",
    )

    out = capsys.readouterr().out
    assert "RAW_JSON_BEGIN" in out
