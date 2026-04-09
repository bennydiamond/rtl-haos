"""Known Device Manager

Centralized orchestrator for all known-device logic.
- Manages state (known_ids, discovery enabled)
- Validates frame acceptance (should process this device?)
- Handles device discovery and removal
- Thread-safe via internal Lock
- Designed to run in processor thread context only
"""

import threading
from typing import Callable, Optional


class KnownDeviceManager:
    """Orchestrates known-device state and validation.
    
    Intended to be accessed exclusively from the processor thread, with narrow
    callback-based interfaces for mqtt_handler interaction.
    """

    def __init__(
        self,
        known_device_store,
        get_discovery_enabled_callback: Callable[[], bool],
        mqtt_cleanup_callback: Callable[[list[str], str], None],
    ):
        """
        Args:
            known_device_store: KnownDeviceStore instance (handles persistence)
            get_discovery_enabled_callback: fn() -> bool (queries mqtt_handler discovery toggle)
            mqtt_cleanup_callback: fn(topics, name) (tells mqtt_handler to clear topics and state)
        """
        self.known_device_store = known_device_store
        self._get_discovery_enabled = get_discovery_enabled_callback
        self._mqtt_cleanup = mqtt_cleanup_callback

        # Load initial known device set from store
        self.known_devices: dict[str, dict] = self.known_device_store.load_devices()
        if self.known_devices:
            print(f"[STARTUP] Loaded {len(self.known_devices)} known device configurations from store.")

        # Protects known_device_ids mutations
        self._lock = threading.Lock()

    def get_discovery_enabled(self) -> bool:
        """Query mqtt_handler for current discovery toggle state."""
        try:
            return self._get_discovery_enabled()
        except Exception as e:
            print(f"[KnownDeviceManager] Error querying discovery state: {e}")
            return False

    def should_process_frame(self, compound_id: str) -> bool:
        """Determine if incoming frame should be processed.

        Returns:
            True if:
              - Discovery is enabled (any device accepted), OR
              - Device is already in known list
            False if:
              - Discovery is disabled AND device not in known list
        """
        if self.get_discovery_enabled():
            return True

        with self._lock:
            return compound_id in self.known_devices

    def add_or_update_device(self, compound_id: str, device_name: str, new_topics: list[str]) -> None:
        """Add a new device or update an existing one with new topics.

        Called by processor after mqtt_handler publishes a new entity.
        Adds device info and topics to known set and persists to JSON.

        Args:
            compound_id: e.g., "rtl433_Model_1234"
            device_name: e.g., "Model 1234"
            new_topics: List of MQTT topics associated with the new entity.
        """
        if not compound_id or not new_topics:
            return

        with self._lock:
            device_data = self.known_devices.setdefault(compound_id, {})
            device_data["name"] = device_name

            existing_topics = set(device_data.get("topics", []))
            updated = False
            for topic in new_topics:
                if topic not in existing_topics:
                    existing_topics.add(topic)
                    updated = True

            if not updated:
                return  # No changes, no need to save.

            device_data["topics"] = sorted(list(existing_topics))

            try:
                self.known_device_store.save_devices(self.known_devices)
            except Exception as e:
                print(f"[KnownDeviceManager] Error persisting discovery: {e}")

    def remove_device(self, compound_id: str) -> None:
        """Remove a device from the known list.

        Called when user removes via MQTT button. Removes from known set,
        persists to JSON, and tells mqtt_handler to clear retained topics.
        """
        compound_id = str(compound_id or "").strip()
        if not compound_id:
            return

        with self._lock:
            if compound_id not in self.known_devices:
                return

            device_data = self.known_devices.pop(compound_id)
            topics_to_delete = device_data.get("topics", [])
            device_name_to_remove = device_data.get("name")

            try:
                self.known_device_store.save_devices(self.known_devices)
            except Exception as e:
                print(f"[KnownDeviceManager] Error persisting removal: {e}")
                # Re-add on persist failure to maintain consistency
                self.known_devices[compound_id] = device_data
                return

        # Tell mqtt_handler to cleanup retained topics
        try:
            self._mqtt_cleanup(topics_to_delete, device_name_to_remove)
        except Exception as e:
            print(f"[KnownDeviceManager] Error during mqtt cleanup: {e}")

    def clear_all_devices(self) -> None:
        """Wipe all known devices from memory and disk.

        Called when the Nuke mechanism is triggered.
        """
        with self._lock:
            self.known_devices.clear()
            try:
                self.known_device_store.save_devices(self.known_devices)
            except Exception as e:
                print(f"[KnownDeviceManager] Error persisting clear_all: {e}")

    def get_known_devices(self) -> set[str]:
        """Return current set of known device compound IDs.

        Used by mqtt_handler to populate UI select options.

        Returns:
            Snapshot of known devices (safe to iterate)
        """
        with self._lock:
            return set(self.known_devices.keys())

    def get_all_known_devices_snapshot(self) -> set[str]:
        """Alias for get_known_devices() for clarity."""
        return self.get_known_devices()
