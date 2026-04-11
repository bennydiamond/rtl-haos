
import types
import pytest

import data_processor
import config


class DummyMQTT:
    def __init__(self):
        self.calls = []

    def send_sensor(self, clean_id, field, value, dev_name, model, is_rtl=True, is_known_device=False):
        self.calls.append((clean_id, field, value, dev_name, model, is_rtl, is_known_device))
        return {
            "accepted": True,
            "published": True,
            "known_device": True,
            "new_device_discovered": not is_known_device,
            "reason": "published",
        }


def test_dispatch_reading_skips_none_value(monkeypatch):
    monkeypatch.setattr(config, "RTL_THROTTLE_INTERVAL", 10, raising=False)
    mqtt = DummyMQTT()
    dp = data_processor.DataProcessor(mqtt)

    dp.dispatch_reading("abc", "x", None, "Dev", "Model", radio_name="R", radio_freq="915M")
    assert dp.buffer == {}
    assert mqtt.calls == []


def test_start_throttle_loop_returns_when_disabled(monkeypatch):
    monkeypatch.setattr(config, "RTL_THROTTLE_INTERVAL", 0, raising=False)
    dp = data_processor.DataProcessor(DummyMQTT())
    # Should just return without looping.
    assert dp.start_throttle_loop() is None


def test_throttle_loop_flushes_and_formats_values(monkeypatch, capsys):
    # Enable throttling and run exactly one flush iteration.
    monkeypatch.setattr(config, "RTL_THROTTLE_INTERVAL", 1, raising=False)

    mqtt = DummyMQTT()
    dp = data_processor.DataProcessor(mqtt)

    # Seed buffer with one device and a few fields.
    dp.dispatch_reading("dev1", "watts", 1, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    dp.dispatch_reading("dev1", "watts", 1, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    # Mean = 1.0 -> should publish int(1) via is_integer() branch.
    dp.dispatch_reading("dev1", "temp", 1, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    dp.dispatch_reading("dev1", "temp", 2, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    # Mean = 1.5 -> stays float (covers non-integer mean branch).
    dp.dispatch_reading("dev1", "battery_ok", 1, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    dp.dispatch_reading("dev1", "battery_ok", 0, "Device 1", "ModelX", radio_name="RTL_A", radio_freq="915M")
    # NON_AVERAGED_NUMERIC_FIELDS -> last value (0)

    calls = {"n": 0}

    def fake_sleep(_):
        calls["n"] += 1
        if calls["n"] == 1:
            return
        raise StopIteration

    monkeypatch.setattr(data_processor.time, "sleep", fake_sleep)

    with pytest.raises(StopIteration):
        dp.start_throttle_loop()

    # Verify sent values
    sent = {(cid, field): value for (cid, field, value, *_rest) in mqtt.calls}
    assert sent[("dev1", "watts")] == 1  # int
    assert sent[("dev1", "temp")] == 1.5  # float
    assert sent[("dev1", "battery_ok")] == 0  # last sample

    out = capsys.readouterr().out
    assert "Flushed" in out
    assert "RTL_A[915M]" in out
