import json
import threading
import types
import pytest
import mqtt_handler
import config



def test_on_message_nuking_empty_payload_returns(monkeypatch):
    h, dummy = _make_handler(monkeypatch)

    h.is_nuking = True
    msg = types.SimpleNamespace(topic="homeassistant/sensor/x/config", payload=b"")
    h._on_message(dummy, None, msg)

    assert dummy.published == []


def test_send_sensor_verbose_print_hit(monkeypatch, capsys):
    import config

    h, dummy = _make_handler(monkeypatch)

    # enable verbose printing
    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", True)

    # force a value change while is_rtl=False so the print gate is exercised
    h.send_sensor("aa:bb:cc:dd:ee:ff", "temp_c", 1, "Dev", "Bridge", is_rtl=False)
    h.send_sensor("aa:bb:cc:dd:ee:ff", "temp_c", 2, "Dev", "Bridge", is_rtl=False)

    out = capsys.readouterr().out.lower()
    assert ("-> tx" in out) or ("data" in out)



class DummyClient:
    def __init__(self, *a, **k):
        self.published = []      # list[(topic, payload, retain)]
        self.subscribed = []     # list[topic]
        self.unsubscribed = []   # list[topic]
        self.connected = None
        self.loop_started = False
        self.loop_stopped = False
        self.disconnected = False
        self.userpass = None
        self.will = None

        # callbacks (assigned by handler)
        self.on_connect = None
        self.on_message = None
        self.on_disconnect = None

        self.max_queued = None
        self.max_inflight = None

    def username_pw_set(self, user, password):
        self.userpass = (user, password)

    def will_set(self, topic, payload, retain=False):
        self.will = (topic, payload, retain)

    def publish(self, topic, payload, retain=False):
        self.published.append((topic, payload, retain))

    def subscribe(self, topic):
        self.subscribed.append(topic)

    def unsubscribe(self, topic):
        self.unsubscribed.append(topic)

    def connect(self, host, port):
        self.connected = (host, port)

    def loop_start(self):
        self.loop_started = True

    def loop_stop(self):
        self.loop_stopped = True

    def disconnect(self):
        self.disconnected = True

    def max_queued_messages_set(self, n):
        self.max_queued = n

    def max_inflight_messages_set(self, n):
        self.max_inflight = n


class DummyTimer:
    def __init__(self, interval, fn):
        self.interval = interval
        self.fn = fn
        self.started = False

    def start(self):
        self.started = True
        return self


def _make_handler(monkeypatch):
    # Ensure HomeNodeMQTT() uses our dummy client
    monkeypatch.setattr(mqtt_handler.mqtt, "Client", lambda *a, **k: DummyClient())

    # Keep IDs deterministic
    monkeypatch.setattr(mqtt_handler, "get_system_mac", lambda: "aa:bb:cc:dd:ee:ff")
    monkeypatch.setattr(mqtt_handler, "clean_mac", lambda s: "deadbeef")

    # Avoid real timers
    monkeypatch.setattr(mqtt_handler.threading, "Timer", DummyTimer)

    # Config sanity
    monkeypatch.setattr(config, "ID_SUFFIX", "_T", raising=False)
    monkeypatch.setattr(config, "BRIDGE_NAME", "Bridge", raising=False)
    monkeypatch.setattr(config, "BRIDGE_ID", "bridgeid", raising=False)
    monkeypatch.setattr(config, "RTL_EXPIRE_AFTER", 60, raising=False)
    monkeypatch.setattr(config, "MAIN_SENSORS", ["main_field"], raising=False)
    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", False, raising=False)
    monkeypatch.setattr(config, "DISCOVERY_NEW_DEVICES", True, raising=False)
    if not getattr(config, "KNOWN_DEVICES_PATH", None):
        monkeypatch.setattr(config, "KNOWN_DEVICES_PATH", "", raising=False)
    monkeypatch.setattr(
        config,
        "MQTT_SETTINGS",
        {"host": "localhost", "port": 1883, "user": "u", "pass": "p"},
        raising=False,
    )

    h = mqtt_handler.HomeNodeMQTT(version="vtest")
    return h, h.client


def _last_published_json(client, topic_prefix):
    matches = [(t, p, r) for (t, p, r) in client.published if t.startswith(topic_prefix)]
    assert matches, f"Expected publish to {topic_prefix}, got: {client.published}"
    t, payload, _retain = matches[-1]
    return t, json.loads(payload)


def test_send_sensor_routes_through_mqtt_handler_thread(monkeypatch):
    h, _c = _make_handler(monkeypatch)
    h.start()

    try:
        worker_ran = threading.Event()
        worker_tid = {"value": None}

        def fake_impl(*args, **kwargs):
            worker_tid["value"] = threading.get_ident()
            worker_ran.set()
            return {
                "accepted": True,
                "published": True,
                "reason": "queued_ok",
                "topics": [],
            }

        monkeypatch.setattr(h, "_send_sensor_impl", fake_impl)

        status = h.send_sensor_sync(
            "aa:bb:cc:dd:ee:ff",
            "temp_c",
            21,
            "Dev",
            "Bridge",
            is_rtl=False,
        )

        assert status["reason"] == "queued_ok"
        assert worker_ran.wait(timeout=1.0)
        assert worker_tid["value"] is not None
        # Must have executed on the mqtt-handler thread, not the calling thread.
        assert worker_tid["value"] != threading.get_ident()

        worker_ran.clear()
        h._mqtt_connected = True
        queued = h.send_sensor(
            "aa:bb:cc:dd:ee:ff",
            "temp_c",
            22,
            "Dev",
            "Bridge",
            is_rtl=False,
        )
        assert queued["reason"] == "queued"
        assert worker_ran.wait(timeout=1.0)
    finally:
        h.stop()


def test_on_connect_success_subscribes_and_publishes_buttons(monkeypatch):
    h, c = _make_handler(monkeypatch)

    h._on_connect(c, None, None, rc=0)

    # availability online
    assert any(t.endswith("/availability") and p == "online" and r is True for (t, p, r) in c.published)

    # command topics set + subscribed
    assert hasattr(h, "nuke_command_topic")
    assert hasattr(h, "restart_command_topic")
    assert hasattr(h, "discovery_command_topic")
    assert hasattr(h, "discovery_state_topic")
    assert h.nuke_command_topic in c.subscribed
    assert h.restart_command_topic in c.subscribed
    assert h.discovery_command_topic in c.subscribed

    # buttons published
    assert any(t.startswith("homeassistant/button/rtl_bridge_nuke_T/config") for (t, _, _) in c.published)
    assert any(t.startswith("homeassistant/button/rtl_bridge_restart_T/config") for (t, _, _) in c.published)
    assert any(t.startswith("homeassistant/switch/rtl_bridge_discovery_new_devices_T/config") for (t, _, _) in c.published)
    assert any(t == h.discovery_state_topic and p in {"ON", "OFF"} and r is True for (t, p, r) in c.published)


def test_on_connect_failure_prints(monkeypatch, capsys):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=5)
    out = capsys.readouterr().out.lower()
    assert "connection failed" in out


def test_on_message_routes_nuke_and_restart(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    called = {"nuke": 0, "restart": 0}

    monkeypatch.setattr(h, "_handle_nuke_press", lambda: called.__setitem__("nuke", called["nuke"] + 1))

    monkeypatch.setattr(mqtt_handler, "trigger_radio_restart", lambda: called.__setitem__("restart", called["restart"] + 1))

    msg_nuke = types.SimpleNamespace(topic=h.nuke_command_topic, payload=b"PRESS")
    h._on_message(c, None, msg_nuke)

    msg_restart = types.SimpleNamespace(topic=h.restart_command_topic, payload=b"PRESS")
    h._on_message(c, None, msg_restart)

    assert called["nuke"] == 1
    assert called["restart"] == 1


def test_on_message_discovery_toggle_switch(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    msg_off = types.SimpleNamespace(topic=h.discovery_command_topic, payload=b"OFF")
    h._on_message(c, None, msg_off)
    assert h.allow_new_device_discovery is False
    assert any(t == h.discovery_state_topic and p == "OFF" and r is True for (t, p, r) in c.published)

    msg_on = types.SimpleNamespace(topic=h.discovery_command_topic, payload=b"ON")
    h._on_message(c, None, msg_on)
    assert h.allow_new_device_discovery is True
    assert any(t == h.discovery_state_topic and p == "ON" and r is True for (t, p, r) in c.published)


def test_on_message_known_devices_dropdown_select(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    msg = types.SimpleNamespace(topic=h.known_devices_command_topic, payload=b"rtl433_Test_123")
    h._on_message(c, None, msg)

    assert h.selected_device_to_remove == "rtl433_Test_123"
    assert any(t == h.known_devices_state_topic and p == "rtl433_Test_123" and r is True for (t, p, r) in c.published)


def test_on_message_remove_device_button(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    called_with = []
    h.remove_device_callback = lambda t: called_with.append(t)
    
    h.selected_device_to_remove = "rtl433_Test_123"
    msg = types.SimpleNamespace(topic=h.remove_device_command_topic, payload=b"PRESS")
    h._on_message(c, None, msg)

    assert called_with == ["rtl433_Test_123"]
    assert h.selected_device_to_remove == "No device selected"
    assert any(t == h.known_devices_state_topic and p == "No device selected" and r is True for (t, p, r) in c.published)

def test_on_message_before_connect_is_caught(monkeypatch, capsys):
    h, c = _make_handler(monkeypatch)

    # Fresh handler has no nuke_command_topic/restart_command_topic yet
    msg = types.SimpleNamespace(topic="anything", payload=b"{}")
    h._on_message(c, None, msg)

    out = capsys.readouterr().out.lower()
    assert "error handling message" in out


def test_nuke_scan_deletes_rtl_haos_and_skips_button_topics(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    h.is_nuking = True

    # empty payload -> ignored
    msg_empty = types.SimpleNamespace(topic="homeassistant/sensor/x/config", payload=b"")
    h._on_message(c, None, msg_empty)
    assert not any(t == "homeassistant/sensor/x/config" and p == "" for (t, p, _) in c.published)

    # rtl-haos manufacturer but button topic -> MUST skip
    payload = json.dumps({"device": {"manufacturer": "rtl-haos"}}).encode("utf-8")
    msg_skip = types.SimpleNamespace(topic="homeassistant/button/rtl_bridge_nuke_T/config", payload=payload)
    h._on_message(c, None, msg_skip)
    assert not any(t == msg_skip.topic and p == "" and r is True for (t, p, r) in c.published)

    # normal rtl-haos entity -> delete publish retained empty payload
    msg_del = types.SimpleNamespace(topic="homeassistant/sensor/rtl_haos/some_entity/config", payload=payload)
    h._on_message(c, None, msg_del)
    assert any(t == msg_del.topic and p == "" and r is True for (t, p, r) in c.published)


def test_handle_nuke_press_timeout_and_threshold(monkeypatch, capsys):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    # Make threshold small for test
    h.NUKE_THRESHOLD = 2
    h.NUKE_TIMEOUT = 5.0

    now = {"t": 100.0}
    monkeypatch.setattr(mqtt_handler.time, "time", lambda: now["t"])

    detonated = {"n": 0}
    monkeypatch.setattr(h, "nuke_all", lambda: detonated.__setitem__("n", detonated["n"] + 1))

    h._handle_nuke_press()
    out1 = capsys.readouterr().out.lower()
    assert "press" in out1 and "more times" in out1
    assert detonated["n"] == 0

    # second press within timeout -> detonate
    now["t"] += 1.0
    h._handle_nuke_press()
    capsys.readouterr()
    assert detonated["n"] == 1

    # timeout reset branch
    now["t"] += 10.0
    h._handle_nuke_press()
    capsys.readouterr()
    assert h.nuke_counter == 1


def test_nuke_all_and_stop_scan(monkeypatch):
    h, c = _make_handler(monkeypatch)
    h._on_connect(c, None, None, rc=0)

    # Seed internal sets so stop scan clears them
    h.discovery_published.add("x")
    h.last_sent_values["k"] = "v"
    h.tracked_devices.add("d")

    called = []
    h.on_nuke_callback = lambda: called.append(True)

    h.nuke_all()
    assert h.is_nuking is True
    assert "homeassistant/+/+/config" in c.subscribed

    h._stop_nuke_scan()
    assert h.is_nuking is False
    assert "homeassistant/+/+/config" in c.unsubscribed
    assert h.discovery_published == set()
    assert h.last_sent_values == {}
    assert h.tracked_devices == set()

    assert called == [True]

    # availability restored online and buttons republished
    assert any(t.endswith("/availability") and p == "online" and r is True for (t, p, r) in c.published)
    assert any(t.startswith("homeassistant/button/rtl_bridge_nuke_T/config") for (t, _, _) in c.published)
    assert any(t.startswith("homeassistant/button/rtl_bridge_restart_T/config") for (t, _, _) in c.published)


def test_cleanup_device_discovered_topics(monkeypatch):
    h, c = _make_handler(monkeypatch)
    
    h.discovery_published.add("dev1_temp")
    h._discovery_sig["dev1_temp"] = "sig"
    h.last_sent_values["dev1_temp"] = "20"
    h.tracked_devices.add("Device1")
    
    topics = [
        "homeassistant/sensor/dev1_temp/config",
        "home/rtl_devices/rtl433_Model_dev1/temp"
    ]
    h.cleanup_device_discovered_topics(topics, "Device1")
    
    assert any(t == topics[0] and p == "" and r is True for (t, p, r) in c.published)
    assert any(t == topics[1] and p == "" and r is True for (t, p, r) in c.published)
    assert "dev1_temp" not in h.discovery_published
    assert "dev1_temp" not in h._discovery_sig
    assert "dev1_temp" not in h.last_sent_values
    assert "Device1" not in h.tracked_devices


def test_publish_discovery_branches_state_class_and_expire(monkeypatch):
    h, c = _make_handler(monkeypatch)

    # Patch FIELD_META to force multiple state_class branches + a bad tuple to hit ValueError fallback
    monkeypatch.setattr(
        mqtt_handler,
        "FIELD_META",
        {
            "gas_field": (None, "gas", "mdi:meter-gas", "Gas Field"),
            "temp_field": ("°C", "temperature", "mdi:thermometer", "Temp Field"),
            "wind_dir": (None, "wind_direction", "mdi:compass", "Wind Dir"),
            "radio_status": (None, "none", "mdi:radio", "Radio Status"),
            "bad_meta": ("u", "temperature", "mdi:x"),  # wrong length -> ValueError path
        },
        raising=False,
    )

    # 1) gas -> total_increasing, device_model==BRIDGE_NAME -> sw_version present
    h._publish_discovery(
        sensor_name="gas_field",
        state_topic="home/x/gas_field",
        unique_id="abc_gas_field",
        device_name="Bridge (deadbeef)",
        device_model=config.BRIDGE_NAME,
        compound_id=f"rtl433_{config.BRIDGE_NAME}_abc",
        friendly_name_override=None,
    )
    topic, payload = _last_published_json(c, "homeassistant/sensor/")
    assert payload.get("state_class") == "total_increasing"
    assert payload["device"].get("sw_version") == "vtest"
    assert payload.get("expire_after") == config.RTL_EXPIRE_AFTER

    # 2) temperature -> measurement, MAIN_SENSORS -> entity_category omitted/None
    h._publish_discovery(
        sensor_name="main_field",  # in MAIN_SENSORS -> entity_cat None
        state_topic="home/x/main_field",
        unique_id="abc_main_field",
        device_name="SomeDevice",
        device_model="NotBridge",
        compound_id="rtl433_NotBridge_abc",
        friendly_name_override="Override Name",
    )
    _t2, p2 = _last_published_json(c, "homeassistant/sensor/")
    assert p2["name"] == "Override Name"
    assert "entity_category" not in p2  # should be omitted when None
    assert p2["device"].get("via_device") == f"rtl433_{config.BRIDGE_NAME}_{config.BRIDGE_ID}"

    # 3) wind_direction -> measurement_angle
    h._publish_discovery(
        sensor_name="wind_dir",
        state_topic="home/x/wind_dir",
        unique_id="abc_wind_dir",
        device_name="Dev",
        device_model="NotBridge",
        compound_id="rtl433_NotBridge_abc",
        friendly_name_override=None,
    )
    _t3, p3 = _last_published_json(c, "homeassistant/sensor/")
    assert p3.get("state_class") == "measurement_angle"

    # 4) radio_status_* special naming + entity_category None + no expire_after
    h._publish_discovery(
        sensor_name="radio_status_0",
        state_topic="home/x/radio_status_0",
        unique_id="abc_radio_status_0",
        device_name="Dev",
        device_model="NotBridge",
        compound_id="rtl433_NotBridge_abc",
        friendly_name_override=None,
    )
    _t4, p4 = _last_published_json(c, "homeassistant/sensor/")
    assert "expire_after" not in p4
    assert "entity_category" not in p4
    assert "Radio Status" in p4["name"]

    # 5) ValueError fallback meta (bad tuple)
    h._publish_discovery(
        sensor_name="bad_meta",
        state_topic="home/x/bad_meta",
        unique_id="abc_bad_meta",
        device_name="Dev",
        device_model="NotBridge",
        compound_id="rtl433_NotBridge_abc",
        friendly_name_override=None,
    )
    _t5, p5 = _last_published_json(c, "homeassistant/sensor/")
    # Should still publish a payload even with bad meta shape
    assert p5["state_topic"] == "home/x/bad_meta"

    # 6) idempotency: same unique_id should not republish
    before = len(c.published)
    h._publish_discovery(
        sensor_name="gas_field",
        state_topic="home/x/gas_field",
        unique_id="abc_gas_field",
        device_name="Bridge (deadbeef)",
        device_model=config.BRIDGE_NAME,
        compound_id=f"rtl433_{config.BRIDGE_NAME}_abc",
    )
    assert len(c.published) == before


def test_send_sensor_value_none_and_verbose_and_no_resend(monkeypatch, capsys):
    h, c = _make_handler(monkeypatch)

    # minimal meta so publish_discovery works
    monkeypatch.setattr(
        mqtt_handler,
        "FIELD_META",
        {"door": (None, "none", "mdi:door", "Door")},
        raising=False,
    )

    # None -> early return
    h.send_sensor("aa:bb", "door", None, "Dev", "NotBridge", is_rtl=False)
    assert c.published == []

    monkeypatch.setattr(config, "VERBOSE_TRANSMISSIONS", True, raising=False)

    # First send (changed) prints verbose TX and publishes state
    h.send_sensor("aa:bb", "door", "OPEN", "Dev", "NotBridge", is_rtl=False)
    out = capsys.readouterr().out.lower()
    assert ("-> tx" in out) or ("data" in out)


    state_topic = "home/rtl_devices/rtl433_NotBridge_deadbeef/door"
    assert any(t == state_topic and p == "OPEN" and r is True for (t, p, r) in c.published)

    # Second send same value + is_rtl=False should NOT republish state
    before_state = len([1 for (t, _, _) in c.published if t == state_topic])
    h.send_sensor("aa:bb", "door", "OPEN", "Dev", "NotBridge", is_rtl=False)
    after_state = len([1 for (t, _, _) in c.published if t == state_topic])
    assert after_state == before_state


def test_battery_ok_publishes_binary_sensor_and_inverts_state(monkeypatch):
    h, c = _make_handler(monkeypatch)

    monkeypatch.setattr(
        mqtt_handler,
        "FIELD_META",
        {"battery_ok": (None, "battery", "mdi:battery", "Battery Low")},
        raising=False,
    )

    # battery_ok=1 => battery is OK, but HA battery binary sensor expects OFF=normal
    h.send_sensor("aa:bb", "battery_ok", 1, "Dev", "NotBridge", is_rtl=False)

    # migration helper deletes any older numeric sensor config topic
    assert any(
        t.startswith("homeassistant/sensor/rtl433_NotBridge_deadbeef_battery_ok_T/config") and p == "" and r is True
        for (t, p, r) in c.published
    )

    # Discovery should be under binary_sensor and include device_class battery
    topic, payload = _last_published_json(c, "homeassistant/binary_sensor/")
    assert topic.startswith("homeassistant/binary_sensor/rtl433_NotBridge_deadbeef_battery_ok_T/config")
    assert payload.get("device_class") == "battery"
    assert payload.get("payload_on") == "ON"
    assert payload.get("payload_off") == "OFF"
    assert "unit_of_measurement" not in payload
    assert "state_class" not in payload

    # State should be OFF when battery_ok==1
    state_topic = "home/rtl_devices/rtl433_NotBridge_deadbeef/battery_ok"
    assert any(t == state_topic and p == "OFF" and r is True for (t, p, r) in c.published)

    # battery_ok=0 => battery is low => ON
    h.send_sensor("aa:bb", "battery_ok", 0, "Dev", "NotBridge", is_rtl=False)
    assert any(t == state_topic and p == "ON" and r is True for (t, p, r) in c.published)


def test_start_success_and_failure_and_stop(monkeypatch):
    h, c = _make_handler(monkeypatch)

    # start success
    h.start()
    assert c.connected == ("localhost", 1883)
    assert c.loop_started is True

    # stop hits publish + loop_stop + disconnect
    h.stop()
    assert c.loop_stopped is True
    assert c.disconnected is True
    assert any(t.endswith("/availability") and p == "offline" and r is True for (t, p, r) in c.published)

    # start failure -> SystemExit
    h2, c2 = _make_handler(monkeypatch)
    def boom_connect(_host, _port):
        raise OSError("no broker")
    c2.connect = boom_connect
    with pytest.raises(SystemExit):
        h2.start()


def test_send_sensor_async_drops_while_disconnected(monkeypatch):
    h, _c = _make_handler(monkeypatch)
    h.start()
    try:
        h._mqtt_has_connected_once = True
        h._mqtt_connected = False
        status = h.send_sensor(
            "aa:bb:cc:dd:ee:ff",
            "temp_c",
            20,
            "Dev",
            "Bridge",
            is_rtl=False,
        )
        assert status["reason"] == "dropped_disconnected"
        assert status["accepted"] is False
    finally:
        h.stop()


def test_send_sensor_async_coalesces_when_queue_full(monkeypatch):
    h, _c = _make_handler(monkeypatch)
    h._worker_thread_id = -1  # force async enqueue path from this thread
    h._mqtt_connected = True
    h._publish_queue = mqtt_handler.queue.Queue(maxsize=1)
    h._publish_queue.put_nowait(({"sensor_id": "x"}, None))

    status = h.send_sensor(
        "aa:bb:cc:dd:ee:ff",
        "temp_c",
        21,
        "Dev",
        "Bridge",
        is_rtl=False,
    )
    assert status["reason"] == "queued_coalesced"
    assert len(h._async_coalesced) == 1


def test_send_sensor_sync_returns_queue_full_when_bounded(monkeypatch):
    h, _c = _make_handler(monkeypatch)
    h._worker_thread_id = -1  # force sync enqueue path
    h._mqtt_connected = True
    h._sync_enqueue_timeout_s = 0.01
    h._publish_queue = mqtt_handler.queue.Queue(maxsize=1)
    h._publish_queue.put_nowait(({"sensor_id": "x"}, None))

    status = h.send_sensor_sync(
        "aa:bb:cc:dd:ee:ff",
        "temp_c",
        22,
        "Dev",
        "Bridge",
        is_rtl=False,
    )
    assert status["reason"] == "queue_full"
    assert status["accepted"] is False
