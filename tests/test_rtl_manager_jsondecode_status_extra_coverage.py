import pytest

import rtl_manager


def test_rtl_loop_publishes_status_on_no_supported_devices(mocker):
    """Covers JSONDecodeError branch that maps common rtl_433 errors to HA status."""

    # Stop the outer restart loop after the first pass.
    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError("stop"))

    # Fake rtl_433 subprocess: first line is an error, then EOF.
    proc = mocker.Mock()
    proc.stdout.readline.side_effect = [
        "No supported devices found\n",
        "",
    ]
    proc.poll.return_value = 0
    proc.terminate.return_value = None
    proc.wait.return_value = None
    proc.kill.return_value = None

    mocker.patch("rtl_manager.subprocess.Popen", return_value=proc)

    mqtt = mocker.Mock()
    mqtt.send_sensor = mocker.Mock()

    data_processor = mocker.Mock()

    radio = {"name": "RTL0", "id": "000", "index": 0, "freq": "433.92M"}

    with pytest.raises(InterruptedError):
        rtl_manager.rtl_loop(radio, mqtt, data_processor, sys_id="SYS", sys_model="MODEL")

    # Find at least one status publish that includes the friendly error.
    calls = mqtt.send_sensor.call_args_list
    assert any(
        (
            len(c.args) >= 3
            and c.args[1].startswith("radio_status_")
            and "No RTL-SDR device" in str(c.args[2])
        )
        for c in calls
    )
