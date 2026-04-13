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
from known_device_aliases import KnownDeviceAliases


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
        mqtt_update_select_callback: Callable[[], None] = None,
    ):
        """
        Args:
            known_device_store: KnownDeviceStore instance (handles persistence)
            get_discovery_enabled_callback: fn() -> bool (queries mqtt_handler discovery toggle)
            mqtt_cleanup_callback: fn(topics, name) (tells mqtt_handler to clear topics and state)
            mqtt_update_select_callback: fn() (tells mqtt_handler to republish select options)
        """
        self.known_device_store = known_device_store
        self._get_discovery_enabled = get_discovery_enabled_callback
        self._mqtt_cleanup = mqtt_cleanup_callback
        self._mqtt_update_select = mqtt_update_select_callback

        # Load initial known device set from store
        self.known_devices: dict[str, dict] = self.known_device_store.load_devices()
        alias_bindings: dict[str, dict] = {}

        load_alias_bindings = getattr(self.known_device_store, "load_alias_bindings", None)
        if callable(load_alias_bindings):
            try:
                loaded_bindings = load_alias_bindings() or {}
                alias_bindings = loaded_bindings if isinstance(loaded_bindings, dict) else {}
            except Exception as e:
                print(f"[KnownDeviceManager] Error loading alias_bindings: {e}")

        # Alias class is a member of KnownDeviceManager.
        self.known_device_aliases = KnownDeviceAliases(
            alias_bindings=alias_bindings,
        )
        if self.known_devices:
            print(f"[STARTUP] Loaded {len(self.known_devices)} known device configurations from store.")

        # Protects known_device_ids mutations
        self._lock = threading.Lock()

    @property
    def alias_bindings(self) -> dict[str, dict]:
        return self.known_device_aliases.alias_bindings

    @alias_bindings.setter
    def alias_bindings(self, value: dict[str, dict]) -> None:
        self.known_device_aliases.set_alias_bindings(value)

    @property
    def _alias_device_resolved(self) -> dict[str, dict]:
        return self.known_device_aliases._alias_device_resolved

    def _rebuild_alias_resolution(self) -> None:
        self.known_device_aliases.rebuild()

    def _resolve_alias_locked(self, physical_compound_id: str, default_device_name: str) -> dict:
        return self.known_device_aliases.resolve(physical_compound_id, default_device_name)

    def _save_known_devices_locked(self, error_context: str) -> bool:
        """Persist current known_devices while the caller holds self._lock."""
        try:
            self.known_device_store.save_devices(self.known_devices)
            return True
        except Exception as e:
            print(f"[KnownDeviceManager] Error persisting {error_context}: {e}")
            return False

    def _save_alias_bindings_locked(self, error_context: str) -> bool:
        """Persist current alias bindings while the caller holds self._lock."""
        save_alias_bindings = getattr(self.known_device_store, "save_alias_bindings", None)
        if not callable(save_alias_bindings):
            # Backward compatibility for tests/older stores.
            return True
        try:
            save_alias_bindings(self.alias_bindings)
            return True
        except Exception as e:
            print(f"[KnownDeviceManager] Error persisting {error_context}: {e}")
            return False

    def _notify_select_update(self) -> None:
        if self._mqtt_update_select:
            self._mqtt_update_select()

    def _resolve_alias_name_for_remove_locked(self, token: str) -> str | None:
        token = str(token or "").strip()
        if not token:
            return None
        if token in self.alias_bindings:
            return token

        for alias_name, resolved in self._alias_device_resolved.items():
            if not isinstance(resolved, dict):
                continue
            if token == str(resolved.get("physical_compound_id") or ""):
                return alias_name
            if token == str(resolved.get("compound_id") or ""):
                return alias_name
        return None

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
            # Explicit alias bindings are user intent: accept frames even if the
            # resolved logical id has not been (re)created in known_devices yet.
            if self.known_device_aliases.is_explicit_bound_device(compound_id):
                return True
            if compound_id in self.known_devices:
                return True
            resolved = self._resolve_alias_locked(compound_id, "")
            return str(resolved.get("compound_id") or "") in self.known_devices

    def resolve_device_identity(self, physical_compound_id: str, device_name: str) -> dict:
        """Resolve a physical compound id to a logical id and display name.

        This is used by MQTT discovery/state publishing so entities remain stable
        when a physical device id changes after battery replacement.
        """
        with self._lock:
            resolved = self._resolve_alias_locked(physical_compound_id, device_name)
            resolved["physical_compound_id"] = str(physical_compound_id or "").strip()
            return resolved

    def bind_alias_to_device(
        self,
        alias_name: str,
        device_compound_id: str,
        *,
        virtual_name: str | None = None,
        logical_compound_id: str | None = None,
        remove_previous_device: bool = True,
    ) -> bool:
        """Bind alias to physical device and persist changes.

                Default behavior removes stale physical-topic identities from known
                devices and clears retained topics, including:
                    - the previously bound device (on rebind), and
                    - the target device's current physical identity topics.
        """
        alias_name = str(alias_name or "").strip()
        device_compound_id = str(device_compound_id or "").strip()
        if not alias_name or not device_compound_id:
            return False

        with self._lock:
            prev_alias_binding = self._alias_device_resolved.get(alias_name, {})
            prev_device_for_alias = str(prev_alias_binding.get("physical_compound_id") or "").strip()

            new_alias_bindings = dict(self.alias_bindings)
            entry = dict(new_alias_bindings.get(alias_name, {}))
            entry["device_compound_id"] = device_compound_id
            entry["matches"] = int(entry.get("matches", 0) or 0) + 1

            if virtual_name is not None:
                txt = str(virtual_name).strip()
                if txt:
                    entry["virtual_name"] = txt
                else:
                    entry.pop("virtual_name", None)

            if logical_compound_id is not None:
                txt = str(logical_compound_id).strip()
                if txt:
                    entry["logical_compound_id"] = txt
                else:
                    entry.pop("logical_compound_id", None)

            new_alias_bindings[alias_name] = entry
            self.alias_bindings = new_alias_bindings

            removed_device_records: list[tuple[list[str], str | None]] = []
            # Always remove the target device's physical identity topics so
            # alias identity does not coexist with stale physical discovery.
            if device_compound_id in self.known_devices:
                target_data = self.known_devices.pop(device_compound_id)
                removed_device_records.append(
                    (list(target_data.get("topics", [])), target_data.get("name"))
                )

            if (
                remove_previous_device
                and prev_device_for_alias
                and prev_device_for_alias != device_compound_id
                and prev_device_for_alias in self.known_devices
            ):
                prev_data = self.known_devices.pop(prev_device_for_alias)
                removed_device_records.append(
                    (list(prev_data.get("topics", [])), prev_data.get("name"))
                )

            saved_alias = self._save_alias_bindings_locked("alias_bindings")
            saved_known = self._save_known_devices_locked("alias_bindings_known_devices")
            saved = saved_alias and saved_known

        if not saved:
            return False

        self._notify_select_update()
        for topics_to_delete, device_name_to_remove in removed_device_records:
            try:
                self._mqtt_cleanup(topics_to_delete, device_name_to_remove)
            except Exception as e:
                print(f"[KnownDeviceManager] Error during mqtt cleanup: {e}")
        return True

    def unbind_alias(self, alias_name: str) -> bool:
        """Remove alias binding and persist."""
        alias_name = str(alias_name or "").strip()
        if not alias_name:
            return False

        with self._lock:
            if alias_name not in self.alias_bindings:
                return False
            new_alias_bindings = dict(self.alias_bindings)
            new_alias_bindings.pop(alias_name, None)
            self.alias_bindings = new_alias_bindings
            saved = self._save_alias_bindings_locked("unbind_alias")

        if saved:
            self._notify_select_update()
        return bool(saved)

    def delete_alias_and_bound_device(self, alias_name: str) -> bool:
        """Delete alias and its matched physical device from known devices.

        This is the safe cleanup path for alias retirement: remove the alias
        binding, remove the currently matched known device entry (if any), and
        clear its retained MQTT topics.
        """
        alias_name = str(alias_name or "").strip()
        if not alias_name:
            return False

        with self._lock:
            if alias_name not in self.alias_bindings:
                return False

            resolved_binding = self._alias_device_resolved.get(alias_name, {})
            target_device = str(resolved_binding.get("physical_compound_id") or "").strip()
            target_logical = str(resolved_binding.get("compound_id") or "").strip()
            if not target_device:
                raw_binding = self.alias_bindings.get(alias_name, {})
                target_device = str(
                    raw_binding.get("device_compound_id")
                    or raw_binding.get("physical_compound_id")
                    or raw_binding.get("compound_id")
                    or ""
                ).strip()

            new_alias_bindings = dict(self.alias_bindings)
            new_alias_bindings.pop(alias_name, None)
            self.alias_bindings = new_alias_bindings

            topics_to_delete: list[str] = []
            device_name_to_remove: str | None = None

            def _pop_known(device_id: str) -> None:
                nonlocal device_name_to_remove
                if not device_id or device_id not in self.known_devices:
                    return
                removed = self.known_devices.pop(device_id)
                topics_to_delete.extend(list(removed.get("topics", [])))
                if device_name_to_remove is None:
                    device_name_to_remove = removed.get("name")

            # Remove both physical and logical representations when present.
            _pop_known(target_device)
            _pop_known(target_logical)

            saved_alias = self._save_alias_bindings_locked("delete_alias")
            saved_known = self._save_known_devices_locked("delete_alias_known_device")
            saved = saved_alias and saved_known

        if not saved:
            return False

        self._notify_select_update()
        if topics_to_delete:
            try:
                self._mqtt_cleanup(sorted(set(topics_to_delete)), device_name_to_remove)
            except Exception as e:
                print(f"[KnownDeviceManager] Error during mqtt cleanup: {e}")
        return True

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

            saved = self._save_known_devices_locked("discovery")

        # Trigger callback outside the lock to prevent deadlocks
        if saved:
            self._notify_select_update()

    def remove_device(self, compound_id: str) -> None:
        """Remove a device from the known list.

        Called when user removes via MQTT button. Removes from known set,
        persists to JSON, and tells mqtt_handler to clear retained topics.
        """
        compound_id = str(compound_id or "").strip()
        if not compound_id:
            return

        # Alias-friendly behavior: selecting alias/logical id from the existing
        # remove-device flow performs alias + bound-device cleanup.
        with self._lock:
            alias_name = self._resolve_alias_name_for_remove_locked(compound_id)
        if alias_name:
            self.delete_alias_and_bound_device(alias_name)
            return

        with self._lock:
            if compound_id not in self.known_devices:
                return

            device_data = self.known_devices.pop(compound_id)
            topics_to_delete = device_data.get("topics", [])
            device_name_to_remove = device_data.get("name")

            saved = self._save_known_devices_locked("removal")
            if not saved:
                # Re-add on persist failure to maintain consistency
                self.known_devices[compound_id] = device_data
                return

        # Trigger callback outside the lock
        self._notify_select_update()

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
            saved = self._save_known_devices_locked("clear_all")

        if saved:
            self._notify_select_update()

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

    def get_alias_names(self) -> set[str]:
        """Return current set of alias names used by alias bindings."""
        with self._lock:
            return set(self.alias_bindings.keys())

    def get_removable_device_options(self) -> set[str]:
        """Return removal options for UI.

        Alias-bound devices are shown by alias name to avoid redundant entries.
        """
        with self._lock:
            options = set(self.known_devices.keys())
            options.update(self.alias_bindings.keys())

            for resolved in self._alias_device_resolved.values():
                if not isinstance(resolved, dict):
                    continue
                options.discard(str(resolved.get("physical_compound_id") or ""))
                options.discard(str(resolved.get("compound_id") or ""))

            return {opt for opt in options if opt}
