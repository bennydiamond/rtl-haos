# mqtt_handler.py
"""
FILE: mqtt_handler.py
DESCRIPTION:
  Manages the connection to the MQTT Broker.
  - UPDATED: Removed legacy gas normalization. Now reports RAW meter values (ft3).
"""
import json
import queue
import threading
import sys
import time
# MQTT client (optional during unit tests)
try:
    import paho.mqtt.client as mqtt
    from paho.mqtt.enums import CallbackAPIVersion
except ModuleNotFoundError:  # pragma: no cover
    class CallbackAPIVersion:  # minimal shim
        VERSION2 = 2

    class _DummyMQTTClient:
        def __init__(self, *args, **kwargs):
            pass

        def username_pw_set(self, *_args, **_kwargs):
            pass

        def will_set(self, *_args, **_kwargs):
            pass

        def connect(self, *_args, **_kwargs):
            raise ModuleNotFoundError("paho-mqtt is required to use MQTT")

        def loop_start(self):
            pass

        def loop_stop(self):
            pass

        def disconnect(self):
            pass

        def publish(self, *_args, **_kwargs):
            pass

        def subscribe(self, *_args, **_kwargs):
            pass

        def unsubscribe(self, *_args, **_kwargs):
            pass

    class _DummyMQTTModule:
        Client = _DummyMQTTClient

    mqtt = _DummyMQTTModule()
# Local imports
import config
from device_count import DeviceCountChannel
from utils import clean_mac, get_system_mac
from field_meta import FIELD_META, get_field_meta
from rtl_manager import trigger_radio_restart

# --- Utility meter commodity inference (Itron ERT / rtlamr conventions) ---
# We infer commodity from fields like 'ert_type' (ERT-SCM) and 'MeterType' (SCMplus/IDM).
ERT_TYPE_COMMODITY = {
    "electric": {4, 5, 7, 8},
    "gas": {0, 1, 2, 9, 12},
    "water": {3, 11, 13},
}

def infer_commodity_from_ert_type(value):
    """Return 'electric'|'gas'|'water' for known ERT type values, else None."""
    try:
        t = int(value)
    except (TypeError, ValueError):
        return None
    for commodity, typeset in ERT_TYPE_COMMODITY.items():
        if t in typeset:
            return commodity
    return None

def infer_commodity_from_meter_type(value):
    """Return commodity from textual MeterType fields (e.g., 'Gas', 'Water', 'Electric')."""
    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if v in {"electric", "electricity", "energy", "power"}:
        return "electric"
    if v in {"gas", "natural gas"}:
        return "gas"
    if v in {"water"}:
        return "water"
    return None


def infer_commodity_from_type_field(value):
    """Return commodity from common 'type' fields.

    rtl_433 decoders are inconsistent across meter families:
      - Some publish a textual 'type' like 'electric'/'gas'/'water'
      - Some publish a numeric ERT type under 'type'

    This helper supports both.
    """
    # Numeric ERT-style type
    if isinstance(value, (int, float)):
        return infer_commodity_from_ert_type(int(value))

    if not isinstance(value, str):
        return None
    v = value.strip().lower()
    if v in {"electric", "electricity", "energy", "power"}:
        return "electric"
    if v in {"gas", "natural gas"}:
        return "gas"
    if v in {"water"}:
        return "water"
    return None



def _parse_boolish(value):
    """Best-effort conversion to bool.

    Returns:
      - True / False when the value is clearly interpretable
      - None when it is not
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"1", "true", "on", "yes", "ok", "good"}:
            return True
        if v in {"0", "false", "off", "no", "low", "bad"}:
            return False
    return None

class HomeNodeMQTT:
    def __init__(self, version="Unknown"):
        self.sw_version = version
        self.client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2)
        self.TOPIC_AVAILABILITY = f"home/status/rtl_bridge{config.ID_SUFFIX}/availability"
        self.client.username_pw_set(config.MQTT_SETTINGS["user"], config.MQTT_SETTINGS["pass"])
        self.client.will_set(self.TOPIC_AVAILABILITY, "offline", retain=True)
        
        # Callbacks
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

        # Connection/backpressure settings
        self._mqtt_connected = False
        self._mqtt_has_connected_once = False
        self._app_queue_max = int(getattr(config, "MQTT_APP_QUEUE_MAX", 5000) or 5000)
        self._app_queue_warn = int(getattr(config, "MQTT_APP_QUEUE_WARN", max(1000, int(self._app_queue_max * 0.8))) or max(1000, int(self._app_queue_max * 0.8)))
        self._sync_enqueue_timeout_s = float(getattr(config, "MQTT_SYNC_ENQUEUE_TIMEOUT", 2.0) or 2.0)
        self._drop_async_when_disconnected = bool(getattr(config, "MQTT_DROP_ASYNC_WHEN_DISCONNECTED", True))
        self._coalesce_max = int(getattr(config, "MQTT_ASYNC_COALESCE_MAX", 2000) or 2000)
        self._last_queue_warn_ts = 0.0
        self._warn_interval_s = float(getattr(config, "MQTT_QUEUE_WARN_INTERVAL", 10.0) or 10.0)

        # Configure paho internal buffering limits when available.
        max_paho_queued = int(getattr(config, "MQTT_CLIENT_MAX_QUEUED_MESSAGES", 2000) or 2000)
        max_paho_inflight = int(getattr(config, "MQTT_CLIENT_MAX_INFLIGHT_MESSAGES", 40) or 40)
        set_max_queued = getattr(self.client, "max_queued_messages_set", None)
        if callable(set_max_queued):
            set_max_queued(max_paho_queued)
        set_max_inflight = getattr(self.client, "max_inflight_messages_set", None)
        if callable(set_max_inflight):
            set_max_inflight(max_paho_inflight)

        self.discovery_published = set()
        self.last_sent_values = {}
        self.tracked_devices = set()
        self.allow_new_device_discovery = bool(getattr(config, "DISCOVERY_NEW_DEVICES", True))
        # Channel that delivers the latest device count to the monitor thread.
        self.device_count_channel = DeviceCountChannel(self)

        # Callback triggered when a full Nuke completes (e.g. to wipe known devices)
        self.on_nuke_callback = None

        # Callbacks for Single Device Deletion
        self.get_known_devices_callback = None
        self.remove_device_callback = None
        self.get_bindable_devices_callback = None
        self.bind_alias_callback = None
        self.resolve_device_identity_callback = None
        self.selected_device_to_remove = "No device selected"
        self.selected_device_to_bind = "No device selected"
        self.alias_name_to_bind = ""
        # Display-name → real-id lookup dicts; populated when selects are published.
        self._remove_devices_lookup: dict[str, str] = {}
        self._bind_devices_lookup: dict[str, str] = {}

        # Track one-time migrations (e.g., entity type/domain changes)
        self.migration_cleared = set()

        # Battery alert state (battery_ok -> Battery Low)
        # Keyed by clean_id (device base unique id).
        self._battery_state: dict[str, dict] = {}
        
        # Shared state lock for discovery metadata, tracked devices, and
        # discovery toggle state. RLock keeps helper composition simple.
        self._state_lock = threading.RLock()
        self.discovery_lock = self._state_lock

        # --- Utility meter inference cache (per-device) ---
        # Used to correctly classify generic fields like 'consumption_data' for ERT-SCM endpoints.
        self._commodity_by_device = {}  # compound_id -> 'electric'|'gas'|'water'

        # Remember the last device model we saw per device.
        # Used for model-specific unit overrides (e.g., Neptune-R900 reports gallons).
        self._device_model_by_id: dict[str, str] = {} # compound_id -> model

        # Remember last raw utility readings so we can re-publish state/config
        # once we learn commodity (or unit preferences) from later fields.
        # Key: (compound_id, field) -> raw_value
        self._utility_last_raw = {}

        # Cache the last discovery signature we published per entity so we can
        # safely update HA discovery when metadata changes (e.g., gas -> energy).
        # Key: unique_id_with_suffix -> signature tuple
        self._discovery_sig = {}

        # All send_sensor calls from external threads are routed through a queue
        # so that the entirety of HomeNodeMQTT state runs on a single dedicated
        # MQTT handler thread (started by start()).
        self._publish_queue: queue.Queue = queue.Queue(maxsize=self._app_queue_max)
        self._async_coalesced_lock = threading.Lock()
        self._async_coalesced: dict[tuple, dict] = {}
        self._publish_stop = threading.Event()
        self._mqtt_thread: threading.Thread | None = None
        self._worker_thread_id: int | None = None


        # --- Nuke Logic Variables ---
        self.nuke_counter = 0
        self.nuke_last_press = 0
        self.NUKE_THRESHOLD = 5       
        self.NUKE_TIMEOUT = 5.0       
        self.is_nuking = False        

    def _worker_send_sensor(self, item) -> None:
        args = item.get("request", {})
        result_queue = item.get("result_queue")
        try:
            status = self._send_sensor_impl(**args)
        except Exception as e:
            status = {
                "accepted": False,
                "published": False,
                "reason": f"worker_exception: {type(e).__name__}",
                "topics": [],
            }
            print(f"[MQTT] send_sensor worker error: {e}")

        if result_queue is not None:
            result_queue.put(status)

    def _worker_publish_known_devices_select(self, _item) -> None:
        try:
            self.publish_known_devices_select()
            self.publish_bind_devices_select()
        except Exception as e:
            print(f"[MQTT] publish_known_devices_select worker error: {e}")

    def _worker_cleanup_device_topics(self, item) -> None:
        try:
            self.cleanup_device_discovered_topics(
                item.get("topics_to_delete", []),
                item.get("device_name_to_remove"),
            )
        except Exception as e:
            print(f"[MQTT] cleanup_device_topics worker error: {e}")

    def _worker_handle_message(self, item) -> None:
        try:
            self._on_message_impl(
                item.get("client"),
                item.get("userdata"),
                item.get("topic", ""),
                item.get("payload", b""),
            )
        except Exception as e:
            print(f"[MQTT] handle_message worker error: {e}")

    def _worker_stop_nuke_scan(self, _item) -> None:
        try:
            self._stop_nuke_scan_impl()
        except Exception as e:
            print(f"[MQTT] stop_nuke_scan worker error: {e}")

    def _worker_on_connect_success(self, _item) -> None:
        try:
            self._on_connect_success_impl()
        except Exception as e:
            print(f"[MQTT] on_connect_success worker error: {e}")

    def _worker_on_disconnect(self, _item) -> None:
        try:
            self._on_disconnect_impl()
        except Exception as e:
            print(f"[MQTT] on_disconnect worker error: {e}")

    def _worker_mqtt_publish(self, item) -> None:
        topic = item.get("topic")
        payload = item.get("payload")
        retain = bool(item.get("retain", False))
        result_queue = item.get("result_queue")
        ok = True
        try:
            self.client.publish(topic, payload, retain=retain)
        except Exception as e:
            ok = False
            print(f"[MQTT] mqtt_publish worker error: {e}")
        if result_queue is not None:
            result_queue.put(ok)

    def _worker_mqtt_subscribe(self, item) -> None:
        topic = item.get("topic")
        result_queue = item.get("result_queue")
        ok = True
        try:
            self.client.subscribe(topic)
        except Exception as e:
            ok = False
            print(f"[MQTT] mqtt_subscribe worker error: {e}")
        if result_queue is not None:
            result_queue.put(ok)

    def _worker_mqtt_unsubscribe(self, item) -> None:
        topic = item.get("topic")
        result_queue = item.get("result_queue")
        ok = True
        try:
            self.client.unsubscribe(topic)
        except Exception as e:
            ok = False
            print(f"[MQTT] mqtt_unsubscribe worker error: {e}")
        if result_queue is not None:
            result_queue.put(ok)

    def _worker_dispatch_item(self, item) -> bool:
        handlers = {
            "send_sensor": self._worker_send_sensor,
            "publish_known_devices_select": self._worker_publish_known_devices_select,
            "cleanup_device_topics": self._worker_cleanup_device_topics,
            "handle_message": self._worker_handle_message,
            "stop_nuke_scan": self._worker_stop_nuke_scan,
            "on_connect_success": self._worker_on_connect_success,
            "on_disconnect": self._worker_on_disconnect,
            "mqtt_publish": self._worker_mqtt_publish,
            "mqtt_subscribe": self._worker_mqtt_subscribe,
            "mqtt_unsubscribe": self._worker_mqtt_unsubscribe,
        }

        if isinstance(item, dict):
            op = item.get("op")
            handler = handlers.get(op)
            if handler is not None:
                handler(item)
                return True

        # Backward compatibility for any legacy queue tuples in tests.
        if isinstance(item, tuple) and len(item) == 2:
            args, result_queue = item
            self._worker_send_sensor({"request": args, "result_queue": result_queue})
            return True

        return False

    def _mqtt_run_loop(self) -> None:
        """Processes the internal work queue.

        This is the body of the dedicated MQTT handler thread started by
        start().  All send_sensor calls from other threads are serialised
        here, ensuring no concurrent mutation of HomeNodeMQTT state.
        """
        self._worker_thread_id = threading.get_ident()
        try:
            while not self._publish_stop.is_set():
                with self._async_coalesced_lock:
                    if self._async_coalesced:
                        _k, coalesced_args = self._async_coalesced.popitem()
                    else:
                        coalesced_args = None

                if coalesced_args is not None:
                    try:
                        self._send_sensor_impl(**coalesced_args)
                    except Exception as e:
                        print(f"[MQTT] send_sensor worker error (coalesced): {e}")
                    continue

                try:
                    item = self._publish_queue.get(timeout=0.25)
                except queue.Empty:
                    continue

                if item is None:
                    break

                self._worker_dispatch_item(item)
        finally:
            self._worker_thread_id = None

    def _make_async_key(self, sensor_id, field, device_name, device_model, is_rtl, friendly_name):
        return (
            str(sensor_id),
            str(field),
            str(device_name),
            str(device_model),
            bool(is_rtl),
            str(friendly_name) if friendly_name is not None else None,
        )

    def _warn_queue_depth(self) -> None:
        try:
            q_depth = self._publish_queue.qsize()
        except Exception:
            q_depth = -1
        with self._async_coalesced_lock:
            coalesced_depth = len(self._async_coalesced)

        if q_depth >= self._app_queue_warn or coalesced_depth >= int(self._coalesce_max * 0.8):
            now = time.time()
            if (now - self._last_queue_warn_ts) >= self._warn_interval_s:
                print(
                    f"[MQTT] WARNING: app queue pressure: queue={q_depth}/{self._app_queue_max}, "
                    f"coalesced={coalesced_depth}/{self._coalesce_max}, connected={self._mqtt_connected}"
                )
                self._last_queue_warn_ts = now

    def _track_device(self, device_name: str) -> None:
        with self._state_lock:
            prev_count = len(self.tracked_devices)
            self.tracked_devices.add(device_name)
            new_count = len(self.tracked_devices)
        if new_count != prev_count:
            self.device_count_channel.push(new_count)

    def _untrack_device(self, device_name: str) -> None:
        with self._state_lock:
            if device_name not in self.tracked_devices:
                return
            self.tracked_devices.remove(device_name)
            new_count = len(self.tracked_devices)
        self.device_count_channel.push(new_count)

    def _reset_tracked_devices(self) -> None:
        with self._state_lock:
            self.tracked_devices.clear()
        self.device_count_channel.push(0)

    def _set_discovery_enabled(self, enabled: bool) -> None:
        with self._state_lock:
            self.allow_new_device_discovery = bool(enabled)

    def _clear_discovery_entries(self, unique_ids) -> None:
        with self.discovery_lock:
            for unique_id in unique_ids:
                self.discovery_published.discard(unique_id)
                self._discovery_sig.pop(unique_id, None)
                self.last_sent_values.pop(unique_id, None)

    def _reset_discovery_state(self) -> None:
        with self.discovery_lock:
            self.discovery_published.clear()
            self.last_sent_values.clear()
            self._discovery_sig.clear()

    def _discard_discovery_published(self, unique_id: str) -> None:
        with self.discovery_lock:
            self.discovery_published.discard(unique_id)

    def _get_last_sent_value(self, unique_id: str):
        with self.discovery_lock:
            return self.last_sent_values.get(unique_id)

    def _set_last_sent_value(self, unique_id: str, value) -> None:
        with self.discovery_lock:
            self.last_sent_values[unique_id] = value

    def _enqueue_mqtt_io(self, item: dict, *, wait: bool = False) -> bool:
        """Run MQTT client I/O on the worker when possible."""
        current_tid = threading.get_ident()
        if self._worker_thread_id is None or current_tid == self._worker_thread_id:
            op = item.get("op")
            if op == "mqtt_publish":
                self.client.publish(item.get("topic"), item.get("payload"), retain=bool(item.get("retain", False)))
                return True
            if op == "mqtt_subscribe":
                self.client.subscribe(item.get("topic"))
                return True
            if op == "mqtt_unsubscribe":
                self.client.unsubscribe(item.get("topic"))
                return True
            return False

        result_queue = queue.Queue() if wait else None
        request = dict(item)
        if result_queue is not None:
            request["result_queue"] = result_queue

        try:
            self._publish_queue.put(request, timeout=self._sync_enqueue_timeout_s)
            self._warn_queue_depth()
        except queue.Full:
            self._warn_queue_depth()
            return False

        if result_queue is None:
            return True
        try:
            return bool(result_queue.get(timeout=5.0))
        except queue.Empty:
            return False

    def _client_publish(self, topic: str, payload, *, retain: bool = False, wait: bool = False) -> bool:
        return self._enqueue_mqtt_io(
            {
                "op": "mqtt_publish",
                "topic": topic,
                "payload": payload,
                "retain": retain,
            },
            wait=wait,
        )

    def _client_subscribe(self, topic: str, *, wait: bool = False) -> bool:
        return self._enqueue_mqtt_io(
            {
                "op": "mqtt_subscribe",
                "topic": topic,
            },
            wait=wait,
        )

    def _client_unsubscribe(self, topic: str, *, wait: bool = False) -> bool:
        return self._enqueue_mqtt_io(
            {
                "op": "mqtt_unsubscribe",
                "topic": topic,
            },
            wait=wait,
        )

    def _enqueue_worker_op(self, item: dict, *, run_inline_if_no_worker: bool = True) -> bool:
        """Enqueue a control operation for the MQTT worker thread."""
        current_tid = threading.get_ident()
        if self._worker_thread_id is None:
            return False
        if current_tid == self._worker_thread_id:
            return False

        try:
            self._publish_queue.put_nowait(item)
            self._warn_queue_depth()
            return True
        except queue.Full:
            self._warn_queue_depth()
            return False

    def _enqueue_named_worker_op(self, item: dict, *, inline_fallback=None, queue_full_fallback=None) -> bool:
        """Enqueue a named worker op with explicit inline and queue-full fallback behavior."""
        current_tid = threading.get_ident()
        if self._worker_thread_id is None or current_tid == self._worker_thread_id:
            if callable(inline_fallback):
                inline_fallback()
            return False

        try:
            self._publish_queue.put_nowait(item)
            self._warn_queue_depth()
            return True
        except queue.Full:
            self._warn_queue_depth()
            if callable(queue_full_fallback):
                queue_full_fallback()
            return False

    def queue_publish_known_devices_select(self) -> None:
        """Schedule known-devices select refresh on the MQTT worker thread."""
        self._enqueue_named_worker_op(
            {"op": "publish_known_devices_select"},
            inline_fallback=self.publish_known_devices_select,
        )

    def queue_cleanup_device_discovered_topics(self, topics_to_delete: list[str], device_name_to_remove: str) -> None:
        """Schedule topic cleanup on the MQTT worker thread."""
        self._enqueue_named_worker_op(
            {
                "op": "cleanup_device_topics",
                "topics_to_delete": topics_to_delete,
                "device_name_to_remove": device_name_to_remove,
            },
            inline_fallback=lambda: self.cleanup_device_discovered_topics(topics_to_delete, device_name_to_remove),
            queue_full_fallback=lambda: self.cleanup_device_discovered_topics(topics_to_delete, device_name_to_remove),
        )

    def _utility_meta_override(self, compound_id, field):
        """Return (unit, device_class, icon, friendly_name) for utility meter readings, or None."""
        commodity = self._commodity_by_device.get(compound_id)
        if not commodity:
            return None

        if commodity == "electric":
            return ("kWh", "energy", "mdi:flash", "Energy Reading")
        if commodity == "gas":
            gas_unit = str(getattr(config.settings, "gas_unit", "ft3") or "ft3").strip().lower()
            if gas_unit in {"ccf", "centum_cubic_feet"}:
                return ("CCF", "gas", "mdi:fire", "Gas Usage")
            return ("ft³", "gas", "mdi:fire", "Gas Usage")
        if commodity == "water":
            # Neptune R900 (protocol 228) typically reports gallons (often in tenths, normalized upstream).
            model = str(self._device_model_by_id.get(compound_id, "") or "").strip()
            if field == "meter_reading" and model.lower().startswith("neptune-r900"):
                return ("gal", "water", "mdi:water-pump", "Water Usage")
            return ("ft³", "water", "mdi:water-pump", "Water Reading")
        return None
    def _utility_normalize_value(self, compound_id: str, field: str, value, device_model: str):
        """Normalize utility readings *after* commodity is known.

        Goals:
          - Electric meters: ERT-SCM/SCMplus typically report hundredths of kWh.
          - Gas meters: ERT-SCM typically reports CCF (hundred cubic feet). Optionally publish ft³.
        """
        commodity = self._commodity_by_device.get(compound_id)
        if not commodity:
            return value

        # Only normalize the main utility total fields.
        if field not in {"Consumption", "consumption", "consumption_data", "meter_reading"}:
            return value

        try:
            v = float(value)
        except (TypeError, ValueError):
            return value

        model = str(device_model or self._device_model_by_id.get(compound_id, "") or "").strip().lower()

        if commodity == "electric":
            # Most ERT-SCM/SCMplus electric meters report hundredths of kWh.
            if model.startswith("ert-scm") or model.startswith("scmplus"):
                return round(v * 0.01, 2)
            return v

        if commodity == "gas":
            # rtlamr/rtl_433 commonly reports the raw counter in ft³ (which is also 0.01 CCF).
            # If you prefer billing units (CCF), we publish CCF by dividing by 100.
            gas_unit = str(getattr(config.settings, "gas_unit", "ft3") or "ft3").strip().lower()
            if gas_unit in {"ccf", "centum_cubic_feet"}:
                return round(v * 0.01, 2)
            # Default: publish ft³
            return v

        # Water (and others): do not normalize here.
        return v


    def _refresh_utility_entities_for_device(self, compound_id: str, clean_id: str, device_name: str, device_model: str) -> None:
        """Re-publish discovery + state for cached utility readings for this device.

        This is used when we learn commodity metadata after the reading was already
        published (e.g., MeterType arrives after Consumption). Without this, HA would
        keep the first-discovered device_class/unit.
        """
        for (cid, field), raw_value in list(self._utility_last_raw.items()):
            if cid != compound_id:
                continue
            # Use is_rtl=False so we only publish if it actually changes.
            self.send_sensor_async(clean_id, field, raw_value, device_name, device_model, is_rtl=False)


    def _on_connect_success_impl(self):
        self._mqtt_connected = True
        self._mqtt_has_connected_once = True
        self._client_publish(self.TOPIC_AVAILABILITY, "online", retain=True)
        print("[MQTT] Connected Successfully.")

        # 1. Subscribe to Nuke Command
        self.nuke_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/nuke/set"
        self._client_subscribe(self.nuke_command_topic)

        # 2. Subscribe to Restart Command
        self.restart_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/restart/set"
        self._client_subscribe(self.restart_command_topic)

        # 3. Subscribe to Discovery Toggle Command
        self.discovery_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/discovery_new_devices/set"
        self.discovery_state_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/discovery_new_devices/state"
        self._client_subscribe(self.discovery_command_topic)

        # 5. Subscribe to Single Device Remove Commands
        self.remove_device_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/remove_device/set"
        self.known_devices_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/known_devices/set"
        self.known_devices_state_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/known_devices/state"
        self._client_subscribe(self.remove_device_command_topic)
        self._client_subscribe(self.known_devices_command_topic)

        # 6. Subscribe to Alias Bind Commands
        self.bind_alias_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_alias/set"
        self.bind_alias_name_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/alias_name/set"
        self.bind_alias_name_state_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/alias_name/state"
        self.bind_devices_command_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_devices/set"
        self.bind_devices_state_topic = f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_devices/state"
        self._client_subscribe(self.bind_alias_command_topic)
        self._client_subscribe(self.bind_alias_name_command_topic)
        self._client_subscribe(self.bind_devices_command_topic)

        # 4. Publish host control entities
        self._publish_nuke_button()
        self._publish_restart_button()
        self._publish_discovery_toggle_switch()
        self._publish_discovery_toggle_state()
        self._publish_remove_device_button()
        self.queue_publish_known_devices_select()
        self._publish_alias_name_text()
        self.publish_bind_devices_select()
        self._publish_bind_alias_button()

    def _on_connect(self, c, u, f, rc, p=None):
        if rc == 0:
            queued = self._enqueue_worker_op({"op": "on_connect_success"}, run_inline_if_no_worker=True)
            if queued:
                return
            self._on_connect_success_impl()
        else:
            self._mqtt_connected = False
            print(f"[MQTT] Connection Failed! Code: {rc}")

    def _on_disconnect_impl(self):
        self._mqtt_connected = False

    def _on_disconnect(self, client, userdata, rc, properties=None):
        queued = self._enqueue_worker_op({"op": "on_disconnect"}, run_inline_if_no_worker=True)
        if queued:
            return
        self._on_disconnect_impl()

    def _on_message(self, client, userdata, msg):
        queued = self._enqueue_worker_op(
            {
                "op": "handle_message",
                "client": client,
                "userdata": userdata,
                "topic": msg.topic,
                "payload": msg.payload,
            },
            run_inline_if_no_worker=True,
        )
        if queued:
            return

        self._on_message_impl(client, userdata, msg.topic, msg.payload)

    def _on_message_impl(self, client, userdata, topic, payload):
        """Handles incoming commands AND Nuke scanning."""
        try:
            # 1. Handle Nuke Button Press
            if topic == self.nuke_command_topic:
                self._handle_nuke_press()
                return

            # 2. Handle Restart Button Press
            if topic == self.restart_command_topic:
                trigger_radio_restart()
                return

            # 3. Handle Discovery Toggle Switch
            discovery_command_topic = getattr(self, "discovery_command_topic", None)
            if discovery_command_topic and topic == discovery_command_topic:
                requested = _parse_boolish(payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload)
                if requested is not None:
                    self._set_discovery_enabled(requested)
                    state_txt = "ON" if requested else "OFF"

                    # Publish state immediately so HA UI reflects the change now.
                    self._publish_discovery_toggle_state()

                    print(f"[MQTT] New-device discovery set to: {state_txt}")
                return

            # 5. Handle Known Devices Dropdown
            known_devices_command_topic = getattr(self, "known_devices_command_topic", None)
            if known_devices_command_topic and topic == known_devices_command_topic:
                selected = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
                self.selected_device_to_remove = selected
                self._client_publish(self.known_devices_state_topic, selected, retain=True)
                return

            # 6. Handle Remove Single Device Button
            remove_device_command_topic = getattr(self, "remove_device_command_topic", None)
            if remove_device_command_topic and topic == remove_device_command_topic:
                target = self.selected_device_to_remove
                if target and target != "No device selected" and callable(self.remove_device_callback):
                    # Map display name back to real token (compound_id or alias name)
                    token = self._remove_devices_lookup.get(target, target)
                    print(f"[MQTT] Requesting removal of single device: {token}")
                    self.remove_device_callback(token)
                    self.selected_device_to_remove = "No device selected"
                    self.publish_known_devices_select()
                return

            # 7. Handle Bind Devices Dropdown
            bind_devices_command_topic = getattr(self, "bind_devices_command_topic", None)
            if bind_devices_command_topic and topic == bind_devices_command_topic:
                selected = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
                self.selected_device_to_bind = selected
                self._client_publish(self.bind_devices_state_topic, selected, retain=True)
                return

            # 8. Handle Alias Name Text Input
            bind_alias_name_command_topic = getattr(self, "bind_alias_name_command_topic", None)
            if bind_alias_name_command_topic and topic == bind_alias_name_command_topic:
                raw_alias = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else str(payload)
                self.alias_name_to_bind = str(raw_alias or "").strip()
                self._client_publish(self.bind_alias_name_state_topic, self.alias_name_to_bind, retain=True)
                return

            # 9. Handle Create/Bind Alias Button
            bind_alias_command_topic = getattr(self, "bind_alias_command_topic", None)
            if bind_alias_command_topic and topic == bind_alias_command_topic:
                target_device = self.selected_device_to_bind
                target_alias = self.alias_name_to_bind
                if (
                    target_device
                    and target_device != "No device selected"
                    and target_alias
                    and callable(self.bind_alias_callback)
                ):
                    # Map display name back to real compound_id
                    compound_id = self._bind_devices_lookup.get(target_device, target_device)
                    print(f"[MQTT] Requesting alias bind: alias='{target_alias}' device='{compound_id}'")
                    ok = bool(self.bind_alias_callback(target_alias, compound_id))
                    if ok:
                        self.selected_device_to_bind = "No device selected"
                        self.alias_name_to_bind = ""
                        self.publish_bind_devices_select()
                        self.publish_known_devices_select()
                        self._client_publish(self.bind_alias_name_state_topic, self.alias_name_to_bind, retain=True)
                return

            # 4. Handle Nuke Scanning (Search & Destroy)
            if self.is_nuking:
                if not payload: return

                try:
                    payload_str = payload.decode("utf-8")
                    data = json.loads(payload_str)
                    
                    # Check Manufacturer Signature
                    device_info = data.get("device", {})
                    manufacturer = device_info.get("manufacturer", "")

                    if "rtl-haos" in manufacturer:
                        # SAFETY: Don't delete the buttons!
                        if "nuke" in topic or "rtl_bridge_nuke" in str(topic): return
                        if "restart" in topic or "rtl_bridge_restart" in str(topic): return

                        print(f"[NUKE] FOUND & DELETING: {topic}")
                        self._client_publish(topic, "", retain=True)
                except Exception:
                    pass

        except Exception as e:
            print(f"[MQTT] Error handling message: {e}")

    def _publish_nuke_button(self):
        """Creates the 'Delete Entities' button."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_nuke{config.ID_SUFFIX}"
        
        payload = {
            "name": "Delete Entities (Press 5x)",
            "command_topic": self.nuke_command_topic,
            "unique_id": unique_id,
            "icon": "mdi:delete-alert",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version
            },
            "availability_topic": self.TOPIC_AVAILABILITY
        }
        
        config_topic = f"homeassistant/button/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)

    def _publish_restart_button(self):
        """Creates the 'Restart Radios' button."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_restart{config.ID_SUFFIX}"
        
        payload = {
            "name": "Restart Radios",
            "command_topic": self.restart_command_topic,
            "unique_id": unique_id,
            "icon": "mdi:restart",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version
            },
            "availability_topic": self.TOPIC_AVAILABILITY
        }
        
        config_topic = f"homeassistant/button/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)

    def _publish_discovery_toggle_switch(self):
        """Creates the 'Allow New Device Discovery' switch."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_discovery_new_devices{config.ID_SUFFIX}"

        payload = {
            "name": "Allow New Device Discovery",
            "command_topic": self.discovery_command_topic,
            "state_topic": self.discovery_state_topic,
            "payload_on": "ON",
            "payload_off": "OFF",
            "unique_id": unique_id,
            "icon": "mdi:radar",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version,
            },
            "availability_topic": self.TOPIC_AVAILABILITY,
        }

        config_topic = f"homeassistant/switch/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)

    def _publish_discovery_toggle_state(self):
        state_txt = "ON" if self._get_discovery_enabled() else "OFF"
        self._client_publish(self.discovery_state_topic, state_txt, retain=True)

    def publish_known_devices_select(self):
        """Creates the 'Known Devices' dropdown."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_known_devices{config.ID_SUFFIX}"

        options = ["No device selected"]
        lookup: dict[str, str] = {}
        if callable(self.get_known_devices_callback):
            try:
                raw = self.get_known_devices_callback()
                if isinstance(raw, dict):
                    lookup = dict(raw)
                    options += sorted(lookup.keys())
                else:
                    options += sorted(list(raw))
            except Exception:
                pass
        self._remove_devices_lookup = lookup

        if self.selected_device_to_remove not in options:
            self.selected_device_to_remove = "No device selected"

        payload = {
            "name": "Select Device to remove",
            "command_topic": getattr(self, "known_devices_command_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/known_devices/set"),
            "state_topic": getattr(self, "known_devices_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/known_devices/state"),
            "options": options,
            "unique_id": unique_id,
            "icon": "mdi:format-list-bulleted",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version,
            },
            "availability_topic": self.TOPIC_AVAILABILITY,
        }

        config_topic = f"homeassistant/select/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)
        
        state_topic = getattr(self, "known_devices_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/known_devices/state")
        self._client_publish(state_topic, self.selected_device_to_remove, retain=True)

    def _publish_remove_device_button(self):
        """Creates the 'Remove Selected Device' button."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_remove_device{config.ID_SUFFIX}"
        
        payload = {
            "name": "Remove Selected Device",
            "command_topic": getattr(self, "remove_device_command_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/remove_device/set"),
            "unique_id": unique_id,
            "icon": "mdi:delete-sweep",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version
            },
            "availability_topic": self.TOPIC_AVAILABILITY
        }
        
        config_topic = f"homeassistant/button/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)

    def publish_bind_devices_select(self):
        """Creates the 'Bind Devices' dropdown for alias create/bind actions."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_bind_devices{config.ID_SUFFIX}"

        options = ["No device selected"]
        lookup: dict[str, str] = {}
        get_devices_cb = self.get_bindable_devices_callback or self.get_known_devices_callback
        if callable(get_devices_cb):
            try:
                raw = get_devices_cb()
                if isinstance(raw, dict):
                    lookup = dict(raw)
                    options += sorted(lookup.keys())
                else:
                    options += sorted(list(raw))
            except Exception:
                pass
        self._bind_devices_lookup = lookup

        if self.selected_device_to_bind not in options:
            self.selected_device_to_bind = "No device selected"

        payload = {
            "name": "Select Device to bind",
            "command_topic": getattr(self, "bind_devices_command_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_devices/set"),
            "state_topic": getattr(self, "bind_devices_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_devices/state"),
            "options": options,
            "unique_id": unique_id,
            "icon": "mdi:link-variant",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version,
            },
            "availability_topic": self.TOPIC_AVAILABILITY,
        }

        config_topic = f"homeassistant/select/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)
        state_topic = getattr(self, "bind_devices_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_devices/state")
        self._client_publish(state_topic, self.selected_device_to_bind, retain=True)

    def _publish_alias_name_text(self):
        """Creates the alias-name text input used by the bind action."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_alias_name{config.ID_SUFFIX}"

        payload = {
            "name": "Alias Name",
            "command_topic": getattr(self, "bind_alias_name_command_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/alias_name/set"),
            "state_topic": getattr(self, "bind_alias_name_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/alias_name/state"),
            "mode": "text",
            "unique_id": unique_id,
            "icon": "mdi:form-textbox",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version,
            },
            "availability_topic": self.TOPIC_AVAILABILITY,
        }

        config_topic = f"homeassistant/text/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)
        state_topic = getattr(self, "bind_alias_name_state_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/alias_name/state")
        self._client_publish(state_topic, self.alias_name_to_bind, retain=True)

    def _publish_bind_alias_button(self):
        """Creates the 'Create/Bind Alias' button."""
        sys_id = get_system_mac().replace(":", "").lower()
        unique_id = f"rtl_bridge_bind_alias{config.ID_SUFFIX}"

        payload = {
            "name": "Create/Bind Alias",
            "command_topic": getattr(self, "bind_alias_command_topic", f"home/status/rtl_bridge{config.ID_SUFFIX}/bind_alias/set"),
            "unique_id": unique_id,
            "icon": "mdi:link-plus",
            "entity_category": "config",
            "device": {
                "identifiers": [f"rtl433_{config.BRIDGE_NAME}_{sys_id}"],
                "manufacturer": "rtl-haos",
                "model": config.BRIDGE_NAME,
                "name": f"{config.BRIDGE_NAME} ({sys_id})",
                "sw_version": self.sw_version,
            },
            "availability_topic": self.TOPIC_AVAILABILITY,
        }

        config_topic = f"homeassistant/button/{unique_id}/config"
        self._client_publish(config_topic, json.dumps(payload), retain=True)

    def _get_discovery_enabled(self) -> bool:
        """Return current discovery toggle state.
        
        Called by KnownDeviceManager to query discovery setting.
        Thread-safe snapshot via shared state lock.
        """
        with self._state_lock:
            return self.allow_new_device_discovery

    def cleanup_device_discovered_topics(self, topics_to_delete: list[str], device_name_to_remove: str) -> None:
        """Clear all retained MQTT topics for a device and update internal state.
        
        Called by KnownDeviceManager when device is removed.
        """
        if not topics_to_delete:
            return

        unique_ids_to_clear = set()

        for topic in topics_to_delete:
            topic = str(topic or "").strip()
            if not topic:
                continue

            # Publish empty retained message to delete the topic from the broker
            self._client_publish(topic, "", retain=True)

            # Extract unique_id from config topics to clear internal caches
            if topic.startswith("homeassistant/") and topic.endswith("/config"):
                parts = topic.split('/')
                if len(parts) >= 3:
                    unique_ids_to_clear.add(parts[-2])

        try:
            self._clear_discovery_entries(unique_ids_to_clear)

            if device_name_to_remove:
                self._untrack_device(device_name_to_remove)

            print(f"[MQTT] Cleared {len(topics_to_delete)} topics for device '{device_name_to_remove or 'Unknown'}'.")
        except Exception as e:
            print(f"[MQTT] Error clearing device topics: {e}")

    def _handle_nuke_press(self):
        """Counts presses and triggers Nuke if threshold met."""
        now = time.time()
        if now - self.nuke_last_press > self.NUKE_TIMEOUT:
            self.nuke_counter = 0
        
        self.nuke_counter += 1
        self.nuke_last_press = now
        
        remaining = self.NUKE_THRESHOLD - self.nuke_counter
        
        if remaining > 0:
            print(f"[NUKE] Safety Lock: Press {remaining} more times to DETONATE.")
        else:
            self.nuke_all()
            self.nuke_counter = 0

    def nuke_all(self):
        """Activates the Search-and-Destroy protocol."""
        print("\n" + "!"*50)
        print("[NUKE] DETONATED! Scanning MQTT for 'rtl-haos' devices...")
        print("!"*50 + "\n")
        self.is_nuking = True
        self._client_subscribe("homeassistant/+/+/config")
        threading.Timer(5.0, self._schedule_stop_nuke_scan).start()

    def _schedule_stop_nuke_scan(self):
        queued = self._enqueue_worker_op({"op": "stop_nuke_scan"}, run_inline_if_no_worker=True)
        if queued:
            return
        self._stop_nuke_scan_impl()

    def _stop_nuke_scan(self):
        # Backward-compatible alias for tests and direct calls.
        self._stop_nuke_scan_impl()

    def _stop_nuke_scan_impl(self):
        """Stops the scanning process and resets state."""
        self.is_nuking = False
        self._client_unsubscribe("homeassistant/+/+/config")
        
        self._reset_discovery_state()

        self._reset_tracked_devices()

        if callable(self.on_nuke_callback):
            try:
                self.on_nuke_callback()
            except Exception as e:
                print(f"[MQTT] Error in nuke callback: {e}")

        print("[NUKE] Scan Complete. All identified entities removed.")
        self._client_publish(self.TOPIC_AVAILABILITY, "online", retain=True)
        self._publish_nuke_button()
        self._publish_restart_button()
        self._publish_discovery_toggle_switch()
        self._publish_discovery_toggle_state()
        self._publish_remove_device_button()
        self.publish_known_devices_select()
        self._publish_alias_name_text()
        self.publish_bind_devices_select()
        self._publish_bind_alias_button()
        print("[NUKE] Host Entities restored.")

    def start(self):
        # Start the single MQTT handler thread that owns all HomeNodeMQTT state.
        self._publish_stop.clear()
        self._mqtt_thread = threading.Thread(
            target=self._mqtt_run_loop,
            name="mqtt-handler",
            daemon=True,
        )
        self._mqtt_thread.start()

        print(f"[STARTUP] Connecting to MQTT Broker at {config.MQTT_SETTINGS['host']}...")
        try:
            self.client.connect(config.MQTT_SETTINGS["host"], config.MQTT_SETTINGS["port"])
            self.client.loop_start()
        except Exception as e:
            print(f"[CRITICAL] MQTT Connect Failed: {e}")
            self._publish_stop.set()
            try:
                self._publish_queue.put_nowait(None)
            except Exception:
                pass
            join = getattr(self._mqtt_thread, "join", None)
            if callable(join):
                join(timeout=2.0)
            self._mqtt_thread = None
            sys.exit(1)

    def stop(self):
        self._publish_stop.set()
        is_alive = getattr(self._mqtt_thread, "is_alive", None)
        should_join = bool(self._mqtt_thread) and (not callable(is_alive) or is_alive())
        if should_join:
            try:
                self._publish_queue.put_nowait(None)  # unblock queue.get()
            except queue.Full:
                pass
            join = getattr(self._mqtt_thread, "join", None)
            if callable(join):
                join(timeout=2.0)
        self._mqtt_thread = None
        with self._async_coalesced_lock:
            self._async_coalesced.clear()
        self._client_publish(self.TOPIC_AVAILABILITY, "offline", retain=True, wait=True)
        self.client.loop_stop()
        self.client.disconnect()

    def _publish_discovery(
        self,
        sensor_name,
        state_topic,
        unique_id,
        device_name,
        device_model,
        compound_id,
        friendly_name_override=None,
        domain="sensor",
        extra_payload=None,
        meta_override=None,
    ):
        unique_id = f"{unique_id}{config.ID_SUFFIX}"

        with self.discovery_lock:

            default_meta = (None, "none", "mdi:eye", sensor_name.replace("_", " ").title())
            
            if sensor_name.startswith("radio_status"):
                base_meta = FIELD_META.get("radio_status", default_meta)
                unit, device_class, icon, default_fname = base_meta
            else:
                meta = get_field_meta(sensor_name, device_model, base_meta=FIELD_META) or default_meta
                if meta_override is not None:
                    meta = meta_override
                try:
                    unit, device_class, icon, default_fname = meta
                except ValueError:
                    unit, device_class, icon, default_fname = default_meta

            if friendly_name_override:
                friendly_name = friendly_name_override
            elif sensor_name.startswith("radio_status_"):
                suffix = sensor_name.replace("radio_status_", "")
                friendly_name = f"{default_fname} {suffix}"
            else:
                friendly_name = default_fname

            entity_cat = "diagnostic"
            if sensor_name in getattr(config, 'MAIN_SENSORS', []):
                entity_cat = None 
            if sensor_name.startswith("radio_status"):
                entity_cat = None

            # Utility meters should not be categorized as diagnostic.
            if device_class in ["gas", "energy", "water"]:
                entity_cat = None

            device_registry = {
                "identifiers": [compound_id],
                "manufacturer": "rtl-haos",
                "model": device_model,
                "name": device_name 
            }

            if device_model != config.BRIDGE_NAME:
                device_registry["via_device"] = "rtl433_"+config.BRIDGE_NAME+"_"+config.BRIDGE_ID
            
            if device_model == config.BRIDGE_NAME:
                device_registry["sw_version"] = self.sw_version

            payload = {
                "name": friendly_name,
                "state_topic": state_topic,
                "unique_id": unique_id,
                "device": device_registry,
                "icon": icon,
            }

            # Common fields across MQTT discovery platforms
            if device_class != "none":
                payload["device_class"] = device_class
            if entity_cat:
                payload["entity_category"] = entity_cat

            # Sensor-only fields
            if domain == "sensor":
                if unit:
                    payload["unit_of_measurement"] = unit

                if device_class in ["gas", "energy", "water", "monetary", "precipitation"]:
                    payload["state_class"] = "total_increasing"
                if device_class in ["temperature", "humidity", "pressure", "illuminance", "voltage", "wind_speed", "moisture"]:
                    payload["state_class"] = "measurement"
                if device_class in ["wind_direction"]:
                    payload["state_class"] = "measurement_angle"

            if extra_payload:
                payload.update(extra_payload)

            if "version" not in sensor_name.lower() and not sensor_name.startswith("radio_status"):
                # Battery status is often reported infrequently; avoid flapping to "unavailable".
                if sensor_name == "battery_ok":
                    payload["expire_after"] = max(int(config.RTL_EXPIRE_AFTER), 86400)
                else:
                    payload["expire_after"] = config.RTL_EXPIRE_AFTER
            
            payload["availability_topic"] = self.TOPIC_AVAILABILITY

            # Signature for safe updates: if this changes, we re-publish the retained config.
            sig = (
                domain,
                payload.get("device_class"),
                payload.get("unit_of_measurement"),
                payload.get("icon"),
                payload.get("name"),
                payload.get("entity_category"),
                payload.get("state_class"),
            )

            prev_sig = self._discovery_sig.get(unique_id)
            if prev_sig == sig:
                # Already published with identical metadata.
                self.discovery_published.add(unique_id)
                return False, []

            config_topic = f"homeassistant/{domain}/{unique_id}/config"
            self._client_publish(config_topic, json.dumps(payload), retain=True)
            self.discovery_published.add(unique_id)
            self._discovery_sig[unique_id] = sig
            return True, [config_topic, state_topic]

    def send_sensor(
        self,
        sensor_id,
        field,
        value,
        device_name,
        device_model,
        is_rtl=True,
        friendly_name=None,
    ):
        """Async sensor send entrypoint.

        This method only confirms enqueuing when called from non-MQTT threads.
        Use send_sensor_sync when the caller needs discovery topics/reply status.
        """
        return self.send_sensor_async(
            sensor_id,
            field,
            value,
            device_name,
            device_model,
            is_rtl=is_rtl,
            friendly_name=friendly_name,
        )

    def send_sensor_async(
        self,
        sensor_id,
        field,
        value,
        device_name,
        device_model,
        is_rtl=True,
        friendly_name=None,
    ):
        # If the MQTT handler thread is not running (e.g. unit tests that never
        # call start()) or this call is already on the handler thread (reentrant
        # path, e.g. _refresh_utility_entities_for_device), run inline to avoid
        # deadlock on the response queue.
        current_tid = threading.get_ident()
        if self._worker_thread_id is None or current_tid == self._worker_thread_id:
            return self._send_sensor_impl(
                sensor_id,
                field,
                value,
                device_name,
                device_model,
                is_rtl=is_rtl,
                friendly_name=friendly_name,
            )

        if (
            self._drop_async_when_disconnected
            and self._mqtt_has_connected_once
            and not self._mqtt_connected
        ):
            return {
                "accepted": False,
                "published": False,
                "reason": "dropped_disconnected",
                "topics": [],
            }

        request = {
            "sensor_id": sensor_id,
            "field": field,
            "value": value,
            "device_name": device_name,
            "device_model": device_model,
            "is_rtl": is_rtl,
            "friendly_name": friendly_name,
        }
        try:
            self._publish_queue.put_nowait({"op": "send_sensor", "request": request, "result_queue": None})
            self._warn_queue_depth()
            return {
                "accepted": True,
                "published": False,
                "reason": "queued",
                "topics": [],
            }
        except queue.Full:
            key = self._make_async_key(sensor_id, field, device_name, device_model, is_rtl, friendly_name)
            with self._async_coalesced_lock:
                self._async_coalesced[key] = request
                if len(self._async_coalesced) > self._coalesce_max:
                    # Drop one older coalesced key to enforce bounded memory.
                    drop_key = next(iter(self._async_coalesced))
                    if drop_key != key:
                        self._async_coalesced.pop(drop_key, None)
            self._warn_queue_depth()
            return {
                "accepted": True,
                "published": False,
                "reason": "queued_coalesced",
                "topics": [],
            }

    def send_sensor_sync(
        self,
        sensor_id,
        field,
        value,
        device_name,
        device_model,
        is_rtl=True,
        friendly_name=None,
    ):
        """Sync sensor send entrypoint.

        Enqueues work and waits for the MQTT worker result when called from
        non-MQTT threads.
        """
        current_tid = threading.get_ident()
        if self._worker_thread_id is None or current_tid == self._worker_thread_id:
            return self._send_sensor_impl(
                sensor_id,
                field,
                value,
                device_name,
                device_model,
                is_rtl=is_rtl,
                friendly_name=friendly_name,
            )

        result_queue: queue.Queue = queue.Queue()
        # Defensive queue sync: drain any stale response before enqueueing.
        # (A fresh queue should be empty, but this prevents accidental reuse bugs.)
        while True:
            try:
                result_queue.get_nowait()
            except queue.Empty:
                break

        request = {
            "sensor_id": sensor_id,
            "field": field,
            "value": value,
            "device_name": device_name,
            "device_model": device_model,
            "is_rtl": is_rtl,
            "friendly_name": friendly_name,
        }
        try:
            self._publish_queue.put(
                {"op": "send_sensor", "request": request, "result_queue": result_queue},
                timeout=self._sync_enqueue_timeout_s,
            )
        except queue.Full:
            self._warn_queue_depth()
            return {
                "accepted": False,
                "published": False,
                "reason": "queue_full",
                "topics": [],
            }

        try:
            return result_queue.get(timeout=5.0)
        except queue.Empty:
            return {
                "accepted": False,
                "published": False,
                "reason": "send_timeout",
                "topics": [],
            }

    def _send_sensor_impl(
        self,
        sensor_id,
        field,
        value,
        device_name,
        device_model,
        is_rtl=True,
        friendly_name=None,
    ):
        status = {
            "accepted": False,
            "published": False,
            "reason": "",
            "topics": [],
            "resolved_compound_id": "",
        }

        if value is None:
            status["reason"] = "none_value"
            return status

        clean_id = clean_mac(sensor_id) 

        self._track_device(device_name)

        status["accepted"] = True

        physical_compound_id = f"rtl433_{device_model}_{clean_id}"
        compound_id = physical_compound_id
        resolved_device_name = device_name

        resolver = getattr(self, "resolve_device_identity_callback", None)
        if callable(resolver):
            try:
                resolved = resolver(physical_compound_id, device_name) or {}
                resolved_id = str(resolved.get("compound_id") or "").strip()
                if resolved_id:
                    compound_id = resolved_id
                resolved_name = str(resolved.get("device_name") or "").strip()
                if resolved_name:
                    resolved_device_name = resolved_name
            except Exception as e:
                print(f"[MQTT] resolve_device_identity callback error: {e}")

        status["resolved_compound_id"] = compound_id
        
        # Remember model for model-specific discovery/unit overrides.
        self._device_model_by_id[compound_id] = str(device_model)

        unique_id_base = compound_id
        state_topic_base = compound_id

        unique_id = f"{unique_id_base}_{field}"
        state_topic = f"home/rtl_devices/{state_topic_base}/{field}"

        # Field-specific transforms / entity types
        domain = "sensor"
        extra_payload = None
        out_value = value

        # Remember raw utility readings so we can re-publish once commodity metadata is known.
        if field in {"Consumption", "consumption", "consumption_data", "meter_reading"}:
            self._utility_last_raw[(compound_id, field)] = value


        # Commodity-aware normalization for utility meters:
        #  - Electric (ERT-SCM/SCMplus): hundredths of kWh -> kWh
        #  - Gas (ERT-SCM): CCF -> optionally publish ft³ (x100)
        # NOTE: If commodity is unknown, we publish the raw value first and
        #       automatically re-publish once commodity metadata arrives.
        prev_commodity = self._commodity_by_device.get(compound_id)

        commodity_update = None
        if field in {"ert_type", "ertType", "ERTType"}:
            commodity_update = infer_commodity_from_ert_type(value)

        if commodity_update is None and field in {"MeterType", "meter_type", "metertype"}:
            commodity_update = infer_commodity_from_meter_type(value)

        # Some decoders publish commodity hints in a generic 'type' field.
        # Only treat it as a utility hint when it looks like a commodity.
        if commodity_update is None and field in {"type", "Type"}:
            commodity_update = infer_commodity_from_type_field(value)

        if commodity_update and commodity_update != prev_commodity:
            self._commodity_by_device[compound_id] = commodity_update
            # Now that we know commodity, update any utility entities we already published.
            self._refresh_utility_entities_for_device(compound_id, clean_id, resolved_device_name, device_model)

        meta_override = None
        if field in {"Consumption", "consumption", "consumption_data", "meter_reading"}:
            meta_override = self._utility_meta_override(compound_id, field)


        # Apply commodity-aware normalization for utility meter readings.
        if field in {"Consumption", "consumption", "consumption_data", "meter_reading"}:
            out_value = self._utility_normalize_value(compound_id, field, out_value, device_model)

        # battery_ok: 1/True => battery OK, 0/False => battery LOW
        # Home Assistant's binary_sensor device_class "battery" expects:
        #   ON  => low
        #   OFF => normal
        if field == "battery_ok":
            ok = _parse_boolish(value)
            if ok is None:
                return

            now = time.time()
            st = self._battery_state.setdefault(
                compound_id,
                {
                    "latched_low": False,
                    "last_low": None,
                    "ok_candidate_since": None,
                    "ok_since": None,
                },
            )

            # Update latch
            if not ok:
                st["latched_low"] = True
                st["last_low"] = now
                st["ok_candidate_since"] = None
                st["ok_since"] = None
                low = True
            else:
                if st.get("latched_low"):
                    if st.get("ok_candidate_since") is None:
                        st["ok_candidate_since"] = now

                    clear_after = int(getattr(config, "BATTERY_OK_CLEAR_AFTER", 0) or 0)
                    if clear_after <= 0 or (now - st["ok_candidate_since"]) >= clear_after:
                        st["latched_low"] = False
                        st["ok_candidate_since"] = None
                        st["ok_since"] = now
                        low = False
                    else:
                        low = True
                else:
                    # Already OK and not latched
                    if st.get("ok_since") is None:
                        st["ok_since"] = now
                    low = False

            domain = "binary_sensor"
            out_value = "ON" if low else "OFF"
            extra_payload = {"payload_on": "ON", "payload_off": "OFF"}

            # Migration helper: if an older numeric sensor existed, remove its discovery config.
            # Only do this once per runtime to avoid extra traffic.
            unique_id_v2 = f"{unique_id}{config.ID_SUFFIX}"
            if unique_id_v2 not in self.migration_cleared:
                old_sensor_config = f"homeassistant/sensor/{unique_id_v2}/config"
                self._client_publish(old_sensor_config, "", retain=True)
                self._discard_discovery_published(unique_id_v2)
                self.migration_cleared.add(unique_id_v2)

            if friendly_name is None:
                friendly_name = "Battery Low"

        discovery_published_now, topics = self._publish_discovery(
            field,
            state_topic,
            unique_id,
            resolved_device_name,
            device_model,
            compound_id=compound_id,
            friendly_name_override=friendly_name,
            domain=domain,
            extra_payload=extra_payload,
            meta_override=meta_override,
        )

        if topics:
            status["topics"].extend(topics)

        unique_id_v2 = f"{unique_id}{config.ID_SUFFIX}"
        value_changed = (self._get_last_sent_value(unique_id_v2) != out_value) or bool(discovery_published_now)

        if value_changed or is_rtl:
            self._client_publish(state_topic, str(out_value), retain=True)
            self._set_last_sent_value(unique_id_v2, out_value)
            status["published"] = True

            if value_changed:
                # --- NEW: Check Verbosity Setting ---
                if config.VERBOSE_TRANSMISSIONS:
                    print(f" -> TX {resolved_device_name} [{field}]: {out_value}")

        if not status["reason"]:
            status["reason"] = "published" if status["published"] else "accepted_no_state_publish"
        return status