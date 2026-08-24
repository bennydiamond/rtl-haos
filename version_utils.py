"""version_utils.py

Centralized version handling for RTL-HAOS.

Single source of truth remains config.yaml's `version:` field, which should stay
`VER.REV.PATCH` (SemVer-ish) so Home Assistant Supervisor comparisons behave as expected.

For logs and device info, we optionally append SemVer build metadata:
  VER.REV.PATCH+BUILD

Build metadata is ignored for version precedence, so it won't create additional
update notifications or re-ordering purely due to internal rebuilds.
"""

from __future__ import annotations

import os
import re
from typing import Optional, Tuple


_VERSION_LINE_RE = re.compile(r"^\s*version\s*:\s*(?P<val>[^#]+?)\s*(?:#.*)?$", re.IGNORECASE)

# SemVer build metadata identifiers: [0-9A-Za-z-]+ separated by dots.
_BUILD_ID_RE = re.compile(r"[^0-9A-Za-z\-\.]+")


def read_base_version(config_yaml_path: str) -> str:
    """Read the base add-on version from config.yaml (expected: VER.REV.PATCH).

    We intentionally avoid a YAML parser dependency here and just scan lines.
    """
    try:
        with open(config_yaml_path, "r", encoding="utf-8") as f:
            for line in f:
                m = _VERSION_LINE_RE.match(line)
                if not m:
                    continue
                raw = m.group("val").strip()
                # Strip surrounding quotes if present
                raw = raw.strip().strip('"').strip("'")
                return raw
    except Exception:
        pass
    return "Unknown"


def _sanitize_build(build: str) -> Optional[str]:
    """Return a SemVer-safe build metadata string (dot-separated identifiers)."""
    if not build:
        return None

    build = str(build).strip()
    # Users might accidentally include leading "+"; drop it
    if build.startswith("+"):
        build = build[1:]

    # Replace whitespace and other illegal chars with dashes, then clean.
    build = build.replace(" ", "-")
    build = _BUILD_ID_RE.sub("-", build)

    # Collapse duplicate separators and trim.
    build = build.strip(".-")
    build = re.sub(r"\.+", ".", build)
    build = re.sub(r"-+", "-", build)

    # Drop empty identifiers
    parts = [p.strip("-") for p in build.split(".") if p.strip("-")]
    if not parts:
        return None

    return ".".join(parts)


def get_build_metadata() -> Optional[str]:
    """Get internal build metadata, if provided.

    Preferred env var: RTL_HAOS_BUILD
    Compatibility env var: RTL_HAOS_TWEAK (older name)
    """
    build = os.getenv("RTL_HAOS_BUILD") or os.getenv("RTL_HAOS_TWEAK") or ""
    return _sanitize_build(build)


def format_display_version(
    base_version: str, build: Optional[str] = None, prefix: str = "v"
) -> str:
    """Return display version for logs + device info: vVER.REV.PATCH(+BUILD)."""
    if not base_version or base_version == "Unknown":
        return "Unknown"

    base_version = str(base_version).strip()
    v = f"{prefix}{base_version}" if prefix else base_version
    if build:
        v = f"{v}+{build}"
    return v


def get_display_version(config_yaml_path: str, prefix: str = "v") -> str:
    """Convenience: read base version and append build metadata if present."""
    base = read_base_version(config_yaml_path)
    build = get_build_metadata()
    return format_display_version(base, build, prefix=prefix)


def notify_version_major_minor(base_version: str) -> Optional[str]:
    """Derive a SemVer-ish 'notify' version that ignores PATCH/build: MAJOR.MINOR.0.

    Returns None if base_version doesn't look like X.Y.Z.
    """
    m = re.match(r"^\s*(\d+)\.(\d+)\.(\d+)", str(base_version).strip())
    if not m:
        return None
    major, minor = m.group(1), m.group(2)
    return f"{major}.{minor}.0"
