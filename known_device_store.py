"""Persistence helper for known device IDs.

This module owns all file I/O for known device tracking state.
"""

from __future__ import annotations

import json
import os
import tempfile


class KnownDeviceStore:
    """Load and save known device IDs from/to JSON on disk."""

    def __init__(self, path: str):
        self.path = str(path or "").strip()

    def _load_raw(self) -> dict:
        if not self.path:
            return {}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except FileNotFoundError:
            return {}
        except Exception:
            return {}
        return raw if isinstance(raw, dict) else {}

    def load_devices(self) -> dict[str, dict]:
        """Return known device data from disk.

        Accepted on-disk formats:
                    - {"devices": {"id1": {}, "id2": {"last_seen": 123}}}
        """
        raw = self._load_raw()

        devices = raw.get("devices") if isinstance(raw, dict) else None
        if not isinstance(devices, dict):
            return {}

        return {k: v for k, v in devices.items() if isinstance(v, dict)}

    def load_alias_bindings(self) -> dict[str, dict]:
        """Return alias->device binding set from disk.

        Expected shape:
          {
            "alias_bindings": {
              "my_alias": {
                "device_compound_id": "rtl433_Model_1234",
                "matches": 10,
                "virtual_name": "Backyard Sensor",
                "logical_compound_id": "rtl433_virtual_backyard_sensor_ab12"
              }
            }
          }
        """
        raw = self._load_raw()
        alias_bindings = raw.get("alias_bindings") if isinstance(raw, dict) else None
        if not isinstance(alias_bindings, dict):
            return {}

        out: dict[str, dict] = {}
        for alias_name, binding in alias_bindings.items():
            if not isinstance(alias_name, str) or not isinstance(binding, dict):
                continue
            out[alias_name] = dict(binding)
        return out

    def save_devices(self, devices: dict[str, dict]) -> None:
        """Persist known IDs with atomic replace semantics.

        On disk we store one object per device so metadata can be added later
        without changing the top-level file shape.
        """
        if not self.path:
            return

        parent_dir = os.path.dirname(self.path) or "."
        
        # Preserve non-device top-level keys (e.g. aliases) while updating devices.
        payload = self._load_raw()
        sorted_devices = {k: devices[k] for k in sorted(devices.keys())}
        payload["devices"] = sorted_devices

        os.makedirs(parent_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=parent_dir,
            delete=False,
            prefix="known_devices_",
            suffix=".json",
        ) as tmp:
            json.dump(payload, tmp, sort_keys=True, indent=2)
            tmp.write("\n")
            tmp_path = tmp.name

        os.replace(tmp_path, self.path)

    def save_alias_bindings(self, alias_bindings: dict[str, dict]) -> None:
        """Persist alias binding set while preserving other top-level keys."""
        if not self.path:
            return

        parent_dir = os.path.dirname(self.path) or "."

        payload = self._load_raw()
        sorted_aliases = {k: alias_bindings[k] for k in sorted(alias_bindings.keys())}
        payload["alias_bindings"] = sorted_aliases

        os.makedirs(parent_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=parent_dir,
            delete=False,
            prefix="known_devices_",
            suffix=".json",
        ) as tmp:
            json.dump(payload, tmp, sort_keys=True, indent=2)
            tmp.write("\n")
            tmp_path = tmp.name

        os.replace(tmp_path, self.path)