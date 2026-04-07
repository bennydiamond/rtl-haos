# tests/test_host_entities_regression.py
import json
import pytest


class ImmediateThread:
    """
    Deterministic threading for unit tests:
    runs the target synchronously when .start() is called.
    """
    def __init__(self, target=None, args=(), kwargs=None, daemon=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}
        self.daemon = daemon

    def start(self):
        if self._target:
            self._target(*self._args, **self._kwargs)


def _run_main_and_exit_fast(mocker, main_module, max_sleep_calls=6):
    """
    main.main() sleeps during startup + forever-loop.
    Patch main.time.sleep to raise KeyboardInterrupt after N calls so tests exit.
    """
    calls = {"n": 0}

    def fake_sleep(_seconds):
        calls["n"] += 1
        if calls["n"] >= max_sleep_calls:
            raise KeyboardInterrupt()

    mocker.patch.object(main_module.time, "sleep", side_effect=fake_sleep)


def test_main_sets_slot_for_missing_ids_in_manual_mode(mocker):
    """
    Regression: if a radio dict has no 'id', main.py must still assign a stable
    sequential slot so radio_status naming can fall back to slot.
    """
    import main
    import config

    # Make startup cheap/deterministic
    mocker.patch.object(main, "get_version", return_value="vtest")
    mocker.patch.object(main, "show_logo", lambda *_: None)
    mocker.patch.object(main, "check_dependencies", lambda: None)
    mocker.patch.object(main, "discover_rtl_devices", return_value=[])
    mocker.patch.object(main, "validate_radio_config", lambda *_args, **_kwargs: [])
    mocker.patch.object(main, "system_stats_loop", lambda *_args, **_kwargs: None)
    mocker.patch.object(main, "get_system_mac", return_value="aa:bb:cc:dd:ee:ff")

    # Replace threads with synchronous runner
    mocker.patch.object(main.threading, "Thread", ImmediateThread)

    # Stub MQTT + Processor
    class DummyMQTT:
        def __init__(self, version=None):
            self.version = version
            self.device_count_channel = type(
                "_Ch",
                (),
                {
                    "loop": lambda self, *a, **k: None,
                    "start_thread": lambda self, *a, **k: None,
                },
            )()
        def start(self): return
        def stop(self): return

    class DummyProcessor:
        def __init__(self, mqtt): self.mqtt = mqtt
        def start_throttle_loop(self): return

    mocker.patch.object(main, "HomeNodeMQTT", DummyMQTT)
    mocker.patch.object(main, "DataProcessor", DummyProcessor)

    # Manual radios: both missing 'id'
    mocker.patch.object(
        config,
        "RTL_CONFIG",
        [
            {"name": "RadioA", "freq": "433.92M", "rate": "250k", "hop_interval": 0},
            {"name": "RadioB", "freq": "915M", "rate": "250k", "hop_interval": 0},
        ],
    )

    received = []

    def fake_rtl_loop(radio, *_args, **_kwargs):
        received.append(dict(radio))

    mocker.patch.object(main, "rtl_loop", fake_rtl_loop)

    _run_main_and_exit_fast(mocker, main, max_sleep_calls=6)

    try:
        main.main()
    except KeyboardInterrupt:
        pass

    assert len(received) == 2, "Expected both radios to start"
    assert received[0].get("slot") == 0, "RadioA must get slot=0"
    assert received[1].get("slot") == 1, "RadioB must get slot=1"


def test_rtl_loop_publishes_host_radio_status_entity_on_start(mocker):
    """
    Regression: rtl_loop must publish a host-level radio_status_* immediately so:
      - HA creates/keeps the entity
      - After NUKE (retained discovery deleted), it can reappear on the next publish
    """
    import rtl_manager

    # Mock rtl_433 process output: one JSON line then EOF
    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.side_effect = [
        '{"model":"FineOffset","id":123,"temperature_C":20.0,"humidity":50}\n',
        "",
    ]
    mock_proc.poll.side_effect = [None, 1]
    mocker.patch("subprocess.Popen", return_value=mock_proc)

    # Stop the outer infinite restart loop
    mock_sleep = mocker.patch("rtl_manager.time.sleep")
    mock_sleep.side_effect = InterruptedError("Stop Test Loop")

    mock_mqtt = mocker.Mock()
    mock_mqtt.send_sensor = mocker.Mock()

    mock_processor = mocker.Mock()

    # IMPORTANT: include slot so even if id is missing, status can be derived
    radio_cfg = {"name": "TestRadio", "freq": "433.92M", "slot": 0}

    try:
        rtl_manager.rtl_loop(radio_cfg, mock_mqtt, mock_processor, "SYSID", "Bridge")
    except InterruptedError:
        pass

    # We don't care about exact suffix here — just that a host radio_status entity was sent
    status_calls = []
    for c in mock_mqtt.send_sensor.call_args_list:
        args = c.args
        if len(args) >= 3:
            sensor_id, field, _value = args[0], args[1], args[2]
            if sensor_id == "SYSID" and str(field).startswith("radio_status"):
                status_calls.append(c)

    assert status_calls, "rtl_loop must call mqtt_handler.send_sensor(SYSID, 'radio_status_*', ...) at least once"


def test_radio_status_is_republished_after_nuke_scan(mocker, mock_config):
    """
    Regression: after NUKE clears retained discovery and internal caches,
    re-sending a radio_status_* must re-send the discovery config topic again.
    """
    import mqtt_handler
    import config

    # Keep topics stable/predictable in assertions
    mocker.patch.object(config, "ID_SUFFIX", "")

    # Use a deterministic system MAC for the host device registry
    mocker.patch.object(mqtt_handler, "get_system_mac", return_value="aa:bb:cc:dd:ee:ff")

    # Mock MQTT client
    fake_client = mocker.Mock()
    mocker.patch.object(mqtt_handler.mqtt, "Client", return_value=fake_client)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")

    # Simulate successful connect (sets command topics + publishes buttons)
    h._on_connect(fake_client, None, None, 0)

    # Publish a host radio status once
    sys_id = "aabbccddeeff"
    field = "radio_status_0"
    unique_id = f"{sys_id}_{field}"
    config_topic = f"homeassistant/sensor/{unique_id}/config"

    h.send_sensor(
        sensor_id=sys_id,
        field=field,
        value="Scanning...",
        device_name=f"{config.BRIDGE_NAME} ({sys_id})",
        device_model=config.BRIDGE_NAME,
        is_rtl=False,
    )

    first_count = sum(1 for call in fake_client.publish.call_args_list if call.args and call.args[0] == config_topic)
    assert first_count >= 1, "Expected initial discovery publish for radio_status"

    # Simulate NUKE finishing (clears discovery_published + last_sent_values + tracked_devices)
    h._stop_nuke_scan()

    # Re-publish the same status value: should re-send discovery again because caches were cleared
    h.send_sensor(
        sensor_id=sys_id,
        field=field,
        value="Scanning...",
        device_name=f"{config.BRIDGE_NAME} ({sys_id})",
        device_model=config.BRIDGE_NAME,
        is_rtl=False,
    )

    second_count = sum(1 for call in fake_client.publish.call_args_list if call.args and call.args[0] == config_topic)
    assert second_count >= 2, "After NUKE, radio_status discovery must be re-published on next status send"
