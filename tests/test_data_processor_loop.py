# tests/test_data_processor_loop.py
import pytest
from data_processor import DataProcessor
import data_processor

def test_throttle_loop_flushes_numeric_and_string(mocker):
    mocker.patch("config.RTL_THROTTLE_INTERVAL", 1)

    mqtt = mocker.Mock()
    p = DataProcessor(mqtt)

    # buffer numeric + string fields
    p.dispatch_reading("dev1", "temp", 10.0, "Dev", "Model", radio_name="RTL0", radio_freq="433M")
    p.dispatch_reading("dev1", "temp", 20.0, "Dev", "Model", radio_name="RTL0", radio_freq="433M")
    p.dispatch_reading("dev1", "temp", 30.0, "Dev", "Model", radio_name="RTL0", radio_freq="433M")
    p.dispatch_reading("dev1", "state", "Open", "Dev", "Model", radio_name="RTL0", radio_freq="433M")
    p.dispatch_reading("dev1", "state", "Closed", "Dev", "Model", radio_name="RTL0", radio_freq="433M")

    # sleep once (process), then raise to stop loop
    mocker.patch.object(data_processor.time, "sleep", side_effect=[None, KeyboardInterrupt])

    with pytest.raises(KeyboardInterrupt):
        p.start_throttle_loop()

    # mean(10,20,30)=20.0 -> becomes int(20) per code
    # New architecture: no is_known_device parameter (gating happens in processor/manager)
    mqtt.send_sensor.assert_any_call("dev1", "temp", 20, "Dev", "Model", is_rtl=True)
    mqtt.send_sensor.assert_any_call("dev1", "state", "Closed", "Dev", "Model", is_rtl=True)


def test_throttle_loop_no_buffer_sends_nothing(mocker):
    mocker.patch("config.RTL_THROTTLE_INTERVAL", 1)

    mqtt = mocker.Mock()
    p = DataProcessor(mqtt)

    # buffer is empty; loop will continue, so stop it on 2nd sleep
    mocker.patch.object(data_processor.time, "sleep", side_effect=[None, KeyboardInterrupt])

    with pytest.raises(KeyboardInterrupt):
        p.start_throttle_loop()

    mqtt.send_sensor.assert_not_called()
