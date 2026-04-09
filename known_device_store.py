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

    def load_devices(self) -> dict[str, dict]:
        """Return known device data from disk.

        Accepted on-disk formats:
                    - {"devices": {"id1": {}, "id2": {"last_seen": 123}}}
        """
        if not self.path:
            return {}

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                raw = json.load(f)
        except FileNotFoundError:
            return {}

        devices = raw.get("devices") if isinstance(raw, dict) else None
        if not isinstance(devices, dict):
            return {}

        return {k: v for k, v in devices.items() if isinstance(v, dict)}

    def save_devices(self, devices: dict[str, dict]) -> None:
        """Persist known IDs with atomic replace semantics.

        On disk we store one object per device so metadata can be added later
        without changing the top-level file shape.
        """
        if not self.path:
            return

        parent_dir = os.path.dirname(self.path) or "."
        
        # Sort top-level keys for deterministic output
        sorted_devices = {k: devices[k] for k in sorted(devices.keys())}
        payload = {"devices": sorted_devices}

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