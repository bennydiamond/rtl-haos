"""Alias resolution helpers for known-device identity remapping.

This module keeps all alias binding and legacy alias behavior self-contained.
"""

from __future__ import annotations

import re


class KnownDeviceAliases:
    """Encapsulates alias bindings and identity resolution.

    Supports:
    - `alias_bindings`: alias -> binding metadata
    """

    def __init__(
        self,
        alias_bindings: dict[str, dict] | None = None,
    ) -> None:
        self.alias_bindings: dict[str, dict] = (
            dict(alias_bindings) if isinstance(alias_bindings, dict) else {}
        )

        self._device_alias_resolved: dict[str, dict] = {}
        self._alias_device_resolved: dict[str, dict] = {}
        self.rebuild()

    @staticmethod
    def _slugify(value: str) -> str:
        txt = str(value or "").strip().lower()
        txt = re.sub(r"[^a-z0-9]+", "_", txt)
        txt = txt.strip("_")
        return txt or "device"

    @staticmethod
    def _model_from_compound_id(compound_id: str) -> str:
        cid = str(compound_id or "")
        if not cid.startswith("rtl433_"):
            return "virtual"
        parts = cid.split("_", 2)
        if len(parts) < 3:
            return "virtual"
        return parts[1] or "virtual"

    @staticmethod
    def _safe_int(value, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    def _logical_compound_from_alias(self, alias_name: str) -> str:
        alias_slug = self._slugify(alias_name)
        return f"rtl433_virtual_{alias_slug}"

    def set_alias_bindings(self, alias_bindings: dict[str, dict] | None) -> None:
        self.alias_bindings = dict(alias_bindings) if isinstance(alias_bindings, dict) else {}
        self.rebuild()

    def rebuild(self) -> None:
        """Build one-to-one alias/device mapping from alias_bindings.

        Conflict rule: highest `matches` score wins. Ties resolve
        deterministically by alias and device id ordering.
        """
        candidates: list[dict] = []
        for alias_name, binding in self.alias_bindings.items():
            if not isinstance(alias_name, str) or not isinstance(binding, dict):
                continue

            device_compound_id = str(
                binding.get("device_compound_id")
                or binding.get("physical_compound_id")
                or binding.get("compound_id")
                or ""
            ).strip()
            if not device_compound_id:
                continue

            matches = self._safe_int(binding.get("matches"), 0)
            logical_compound_id = str(binding.get("logical_compound_id") or "").strip()
            if not logical_compound_id:
                logical_compound_id = self._logical_compound_from_alias(alias_name)

            virtual_name = str(binding.get("virtual_name") or alias_name).strip() or alias_name

            candidates.append(
                {
                    "alias": alias_name,
                    "device_compound_id": device_compound_id,
                    "matches": matches,
                    "logical_compound_id": logical_compound_id,
                    "virtual_name": virtual_name,
                }
            )

        candidates.sort(
            key=lambda c: (
                -int(c.get("matches", 0)),
                str(c.get("alias", "")),
                str(c.get("device_compound_id", "")),
            )
        )

        used_aliases: set[str] = set()
        used_devices: set[str] = set()
        device_map: dict[str, dict] = {}
        alias_map: dict[str, dict] = {}

        for cand in candidates:
            alias = str(cand["alias"])
            device = str(cand["device_compound_id"])
            if alias in used_aliases or device in used_devices:
                continue

            logical_compound_id = str(cand["logical_compound_id"])
            base_name = str(cand["virtual_name"])
            resolved_name = base_name

            resolved = {
                "alias": alias,
                "physical_compound_id": device,
                "compound_id": logical_compound_id,
                "device_name": resolved_name,
                "matches": int(cand.get("matches", 0)),
            }

            device_map[device] = resolved
            alias_map[alias] = resolved
            used_aliases.add(alias)
            used_devices.add(device)

        self._device_alias_resolved = device_map
        self._alias_device_resolved = alias_map

    def is_explicit_bound_device(self, physical_compound_id: str) -> bool:
        physical_compound_id = str(physical_compound_id or "").strip()
        return physical_compound_id in self._device_alias_resolved

    def resolve(self, physical_compound_id: str, default_device_name: str) -> dict:
        """Resolve physical id to effective logical identity.

        Returns dict with keys: compound_id, device_name.
        """
        physical_compound_id = str(physical_compound_id or "").strip()
        default_device_name = str(default_device_name or "").strip()

        resolved_binding = self._device_alias_resolved.get(physical_compound_id)
        if isinstance(resolved_binding, dict):
            return {
                "compound_id": str(resolved_binding.get("compound_id") or physical_compound_id),
                "device_name": str(resolved_binding.get("device_name") or default_device_name),
            }
        return {
            "compound_id": physical_compound_id,
            "device_name": default_device_name,
        }
