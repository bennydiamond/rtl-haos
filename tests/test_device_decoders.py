import pytest
from unittest.mock import MagicMock
from rtl_manager import rtl_loop


def test_neptune_water_meter_math(mocker):
    """
    CRITICAL: Verifies Neptune R900 consumption is divided by 10.
    """
    # 1. Setup mocks
    mock_mqtt = mocker.Mock()
    mock_processor = mocker.Mock()

    # 2. Simulate the specific JSON from a Neptune meter
    # Note: Raw consumption is 12345, we expect 1234.5
    fake_lines = [
        '{"model": "Neptune-R900", "id": "Meter1", "consumption": 12345, "type": "water"}\n',
        "",
    ]

    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.side_effect = fake_lines
    mock_proc.poll.side_effect = [None, 1]  # Run once, then die
    mocker.patch("subprocess.Popen", return_value=mock_proc)
    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError)  # Break loop

    # 3. Run
    try:
        rtl_loop({"name": "Test"}, mock_mqtt, mock_processor, "sys", "mod")
    except InterruptedError:
        pass

    # 4. Verify the math happened
    # We look for a call to dispatch_reading with value 1234.5
    found = False
    for call in mock_processor.dispatch_reading.call_args_list:
        args = call.args
        # args format: (clean_id, field, value, ...)
        if args[1] == "meter_reading" and args[2] == 1234.5:
            found = True
            break

    assert found, "CRITICAL: Neptune consumption was NOT divided by 10!"


def test_auto_dewpoint_calculation(mocker):
    """
    Ensures Dew Point is calculated if Temp + Humidity are present.
    """
    # 1. Setup mocks
    mock_processor = mocker.Mock()

    # 2. Simulate a device with Temp (C) and Humidity
    # 20C + 50% Hum = ~48.7F Dew Point
    fake_lines = ['{"model": "Acurite", "id": "A1", "temperature_C": 20.0, "humidity": 50}\n', ""]

    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.side_effect = fake_lines
    mock_proc.poll.side_effect = [None, 1]
    mocker.patch("subprocess.Popen", return_value=mock_proc)
    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError)

    # 3. Run
    try:
        rtl_loop({"name": "Test"}, mocker.Mock(), mock_processor, "sys", "mod")
    except InterruptedError:
        pass

    # 4. Verify "dew_point" was dispatched
    found_dp = False
    for call in mock_processor.dispatch_reading.call_args_list:
        if call.args[1] == "dew_point":
            val = call.args[2]
            assert 48.0 < val < 50.0  # Approximate check
            found_dp = True

    assert found_dp, "Dew Point was not auto-calculated!"
