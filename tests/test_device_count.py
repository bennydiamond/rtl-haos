"""Tests for device_count.DeviceCountChannel."""
import queue as _queue

import pytest

from device_count import DeviceCountChannel


# ---------------------------------------------------------------------------
# push() — depth-1 queue behaviour
# ---------------------------------------------------------------------------

def test_push_delivers_value():
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(5)
    assert ch._queue.get_nowait() == 5


def test_push_flushes_stale_value():
    """A second push() before the consumer reads must replace the first value."""
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(3)
    ch.push(7)  # must overwrite 3

    assert ch._queue.get_nowait() == 7
    assert ch._queue.empty(), "Queue must contain at most one item after push"


def test_push_zero_after_nuke():
    """push(0) must be accepted even when queue is empty (NUKE path)."""
    class DummyMQTT:
        def send_sensor(self, *a, **k): pass

    ch = DeviceCountChannel(DummyMQTT())
    ch.push(0)
    assert ch._queue.get_nowait() == 0


# ---------------------------------------------------------------------------
# loop() — consumer thread behaviour
# ---------------------------------------------------------------------------

def _make_channel_with_mock_mqtt(mocker):
    mqtt = mocker.Mock()
    ch = DeviceCountChannel(mqtt)
    return ch, mqtt


def test_loop_publishes_count_from_queue(mocker):
    """loop() must call send_sensor with the value pushed into the queue."""
    ch, mqtt = _make_channel_with_mock_mqtt(mocker)

    # First get() returns 3; second stops the loop.
    ch._queue.get = mocker.Mock(side_effect=[3, InterruptedError("stop")])

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
    """When get() times out (60 s heartbeat), last known count is re-published."""
    ch, mqtt = _make_channel_with_mock_mqtt(mocker)

    # First get() delivers 4; second simulates timeout (Empty); third stops.
    ch._queue.get = mocker.Mock(
        side_effect=[4, _queue.Empty(), InterruptedError("stop")]
    )

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

    mqtt.send_sensor.side_effect = [RuntimeError("mqtt down"), None]
    ch._queue.get = mocker.Mock(side_effect=[1, InterruptedError("stop")])

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
