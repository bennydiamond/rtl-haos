"""Tests for device_count.DeviceCountChannel."""

import pytest

from device_count import DeviceCountChannel


# ---------------------------------------------------------------------------
# push() — latest-value slot behaviour
# ---------------------------------------------------------------------------

def test_push_delivers_value():
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(5)
    assert ch._count_last == 5
    assert ch._count_pending is True
    assert ch._count_event.is_set() is True


def test_push_overwrites_stale_value():
    """A second push() before the consumer reads must replace the first value."""
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(3)
    ch.push(7)  # must overwrite 3

    assert ch._count_last == 7
    assert ch._count_pending is True
    assert ch._count_event.is_set() is True


def test_push_zero_after_nuke():
    """push(0) must be accepted even when no prior update exists (NUKE path)."""
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(0)
    assert ch._count_last == 0
    assert ch._count_pending is True


# ---------------------------------------------------------------------------
# loop() — consumer thread behaviour
# ---------------------------------------------------------------------------

def _make_channel_with_mock_mqtt(mocker):
    mqtt = mocker.Mock()
    ch = DeviceCountChannel(mqtt)
    return ch, mqtt


def test_loop_publishes_count_from_latest_value(mocker):
    """loop() must call send_sensor with the value published via push()."""
    ch, mqtt = _make_channel_with_mock_mqtt(mocker)
    ch.push(3)

    # First wait() wakes on update; second stops the loop.
    ch._count_event.wait = mocker.Mock(side_effect=[True, InterruptedError("stop")])

    with pytest.raises(InterruptedError):
        ch.loop("dev123", "Bridge")

    mqtt.send_sensor.assert_any_call(
        "dev123",
        "sys_device_count",
        3,
        "Bridge (dev123)",
        "Bridge",
        is_rtl=True,
    )


def test_loop_heartbeat_republishes_last_count(mocker):
    """When wait() times out (60 s heartbeat), last known count is re-published."""
    ch, mqtt = _make_channel_with_mock_mqtt(mocker)
    ch.push(4)

    # First wait() wakes on update; second times out; third stops.
    ch._count_event.wait = mocker.Mock(side_effect=[True, False, InterruptedError("stop")])

    with pytest.raises(InterruptedError):
        ch.loop("devA", "Bridge")

    calls = [
        c for c in mqtt.send_sensor.call_args_list
        if c.args[1] == "sys_device_count"
    ]
    counts = [c.args[2] for c in calls]
    assert counts == [4, 4], f"Expected two publishes of 4, got {counts}"


def test_loop_handles_send_sensor_exception(mocker, capsys):
    """Exceptions from send_sensor must be caught and logged; loop must continue."""
    ch, mqtt = _make_channel_with_mock_mqtt(mocker)
    ch.push(1)

    mqtt.send_sensor.side_effect = [RuntimeError("mqtt down"), None]
    ch._count_event.wait = mocker.Mock(side_effect=[True, False, InterruptedError("stop")])

    with pytest.raises(InterruptedError):
        ch.loop("devB", "Bridge")

    out = capsys.readouterr().out
    assert "Device Count update failed" in out


def test_start_thread_uses_factory_and_starts(mocker):
    """start_thread() should construct and start a daemon thread for loop()."""
    ch, _mqtt = _make_channel_with_mock_mqtt(mocker)

    thread = mocker.Mock()
    factory = mocker.Mock(return_value=thread)

    ret = ch.start_thread("devX", "Bridge", thread_factory=factory)

    assert ret is thread
    factory.assert_called_once_with(target=ch.loop, args=("devX", "Bridge"), daemon=True)
    thread.start.assert_called_once_with()
