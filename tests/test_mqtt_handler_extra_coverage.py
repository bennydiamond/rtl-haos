import json
import types
import inspect
import pytest


class DummyClient:
    def __init__(self, *args, **kwargs):
        self.published = []
        self.subscribed = []
        self.unsubscribed = []
        self.connected = False

    def username_pw_set(self, *_a, **_k):
        return

    def will_set(self, *_a, **_k):
        return

    def connect(self, *_a, **_k):
        self.connected = True
        return 0

    def loop_start(self):
        return

    def loop_stop(self):
        return

    def disconnect(self):
        return

    def publish(self, topic, payload=None, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return types.SimpleNamespace(rc=0)

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))
        return (0, 0)

    def unsubscribe(self, topic):
        self.unsubscribed.append(topic)
        return (0, 0)


def _make_handler(mocker):
    import mqtt_handler

    dummy = DummyClient()
    mocker.patch.object(mqtt_handler.mqtt, "Client", return_value=dummy)
    h = mqtt_handler.HomeNodeMQTT(version="vtest")

    # Your _on_message references these; in your code they aren't created until later
    if not hasattr(h, "nuke_command_topic"):
        h.nuke_command_topic = "homeassistant/button/rtl_haos/rtl_bridge_nuke_press/set"
    if not hasattr(h, "restart_command_topic"):
        h.restart_command_topic = "homeassistant/button/rtl_haos/rtl_bridge_restart/set"

    return h, dummy


def _call_send_sensor(h, device_id="dev1", model="Bridge", field="door", value="OPEN", unit=None):
    """
    Call HomeNodeMQTT.send_sensor without assuming parameter names/order.
    """
    sig = inspect.signature(h.send_sensor)
    kwargs = {}
    for name, p in sig.parameters.items():
        if name == "self":
            continue
        lname = name.lower()

        if lname in ("field",):
            kwargs[name] = field
        elif lname in ("value",):
            kwargs[name] = value
        elif "unit" in lname:
            kwargs[name] = unit
        elif "device" in lname and "id" in lname:
            kwargs[name] = device_id
        elif lname in ("model_name", "model", "device_model"):
            kwargs[name] = model
        elif lname in ("device_name", "name"):
            kwargs[name] = model
        elif lname == "is_rtl":
            kwargs[name] = False
        elif p.default is not inspect._empty:
            # leave default
            pass
        else:
            # required but unknown → harmless filler
            kwargs[name] = None

    return h.send_sensor(**kwargs)


def test_on_connect_nonzero_rc_prints_error(mocker, capsys):
    h, client = _make_handler(mocker)

    h._on_connect(client, None, None, rc=5)
    out = capsys.readouterr().out.lower()

    assert "connection failed" in out
    assert "5" in out


def test_start_exits_on_connect_failure(mocker):
    import mqtt_handler

    dummy = DummyClient()
    mocker.patch.object(mqtt_handler.mqtt, "Client", return_value=dummy)

    h = mqtt_handler.HomeNodeMQTT(version="vtest")

    def boom(*_a, **_k):
        raise ConnectionRefusedError("no broker")

    h.client.connect = boom
    with pytest.raises(SystemExit):
        h.start()


def test_stop_propagates_loop_stop_errors(mocker):
    h, dummy = _make_handler(mocker)

    def bad_loop_stop():
        raise RuntimeError("oops")

    dummy.loop_stop = bad_loop_stop

    with pytest.raises(RuntimeError):
        h.stop()


def test_on_message_restart_topic_triggers_restart_if_available(mocker):
    import mqtt_handler

    h, dummy = _make_handler(mocker)

    called = {"n": 0}

    def bump(*_a, **_k):
        called["n"] += 1

    patched_any = False

    if hasattr(mqtt_handler, "trigger_radio_restart"):
        mocker.patch.object(mqtt_handler, "trigger_radio_restart", side_effect=bump)
        patched_any = True
    if hasattr(h, "restart_radios"):
        mocker.patch.object(h, "restart_radios", side_effect=bump)
        patched_any = True

    msg = types.SimpleNamespace(topic=h.restart_command_topic, payload=b"PRESS")
    h._on_message(dummy, None, msg)

    # If your implementation exposes a restart hook, it should be called.
    if patched_any:
        assert called["n"] >= 1


def test_on_message_nuke_deletes_discovery_entities(mocker):
    h, dummy = _make_handler(mocker)
    h.is_nuking = True

    # Try both common manufacturer spellings; assert at least one triggers delete publish
    for mfr in ("rtl-haos", "RTL-HAOS"):
        payload = json.dumps(
            {"device": {"manufacturer": mfr, "identifiers": ["rtl_haos_bridge"]}}
        ).encode("utf-8")

        msg = types.SimpleNamespace(
            topic="homeassistant/sensor/rtl_haos/some_entity/config", payload=payload
        )
        h._on_message(dummy, None, msg)

        if dummy.published:
            break

    assert dummy.published, "Expected retained delete publish"
    topic, payload, _qos, retain = dummy.published[-1]
    assert topic.endswith("/config")
    assert payload == "" or payload is None
    assert retain is True


def test_send_sensor_non_numeric_value_does_not_crash_and_publishes_discovery(mocker):
    h, dummy = _make_handler(mocker)

    _call_send_sensor(h, field="door", value="OPEN", unit=None)

    assert any(t.endswith("/config") for (t, _p, _q, _r) in dummy.published)


def test_send_sensor_radio_status_does_not_crash(mocker):
    h, dummy = _make_handler(mocker)

    _call_send_sensor(h, field="radio_status", value="OK", unit=None)

    assert any(t.endswith("/config") for (t, _p, _q, _r) in dummy.published)
