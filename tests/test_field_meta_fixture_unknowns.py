"""Field-meta regression guard using captured rtl_433 JSON fixtures.

This test is intentionally *fixture-driven*:
  - If no fixtures exist, the test is skipped.
  - If fixtures exist, it asserts that every field we would publish to MQTT
    has a FIELD_META (or model-specific override) entry.

This gives you a simple workflow to keep `field_meta.py` current with the
devices/protocols you actually see in the wild:

  1) Capture a short rtl_433 JSON sample to a file:

       rtl_433 -F json -T 20 > tests/fixtures/rtl433/events.jsonl

     (or use replay mode: rtl_433 -r <fixture> -F json > ...)

  2) Run pytest. Any missing fields are listed with paste-ready stubs.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

import pytest

import config
import rtl_manager
from field_meta import FIELD_META, get_field_meta


_ARRAY_DERIVED_KEY_RE = re.compile(r".+_\d+$")


def _iter_fixture_events(fixtures_dir: Path):
    """Yield dict events from *.jsonl or *.json fixture files."""
    # Prefer jsonl (rtl_433 emits JSON lines); fall back to .json.
    jsonl = sorted(fixtures_dir.glob("*.jsonl")) + sorted(fixtures_dir.glob("*.ndjson"))
    jsons = sorted(fixtures_dir.glob("*.json"))

    for path in jsonl:
        for line in path.read_text(errors="replace").splitlines():
            line = line.strip()
            if not line or not (line.startswith("{") and line.endswith("}")):
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield path, obj

    for path in jsons:
        try:
            obj = json.loads(path.read_text(errors="replace"))
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            yield path, obj
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    yield path, item


def _planned_publish_fields(*, data_raw: dict, model: str) -> list[tuple[str, object]]:
    """Mirror rtl_manager.rtl_loop's publish plan (field names + values)."""
    # Start from a mutable copy to match rtl_loop's in-place mutations.
    data_processed = copy.deepcopy(data_raw)

    # Neptune R900: consumption -> meter_reading; consumption removed.
    if "Neptune-R900" in (model or "") and data_processed.get("consumption") is not None:
        try:
            meter_val = float(data_processed["consumption"]) / 10.0
            # rtl_loop publishes meter_reading separately
            derived = [("meter_reading", meter_val)]
        except Exception:
            derived = []
        data_processed.pop("consumption", None)
    else:
        derived = []

    # SCM/ERT: consumption -> Consumption; consumption removed.
    if (("SCM" in (model or "")) or ("ERT" in (model or ""))) and data_processed.get(
        "consumption"
    ) is not None:
        derived.append(("Consumption", data_processed.get("consumption")))
        data_processed.pop("consumption", None)

    # Dew point: derived from temperature + humidity
    try:
        t_c = data_raw.get("temperature_C")
        if t_c is None and data_raw.get("temperature_F") is not None:
            t_c = (float(data_raw["temperature_F"]) - 32.0) * 5.0 / 9.0
        hum = data_raw.get("humidity")
        if isinstance(t_c, (int, float)) and isinstance(hum, (int, float)):
            dp_f = rtl_manager.calculate_dew_point(t_c, hum)
            if dp_f is not None:
                derived.append(("dew_point", dp_f))
    except Exception:
        # If any fixture line is weird, don't explode the whole test.
        pass

    flat = rtl_manager.flatten(data_processed)
    skip = set(getattr(config, "SKIP_KEYS", []) or [])

    planned: list[tuple[str, object]] = []

    # Derived first, then normal flattened fields.
    planned.extend(derived)
    for key, value in flat.items():
        if key in skip:
            continue
        if key in ["temperature_C", "temp_C"] and isinstance(value, (int, float)):
            planned.append(("temperature", round(value * 1.8 + 32.0, 1)))
        elif key in ["temperature_F", "temp_F", "temperature"] and isinstance(value, (int, float)):
            planned.append(("temperature", value))
        else:
            planned.append((key, value))

    # Deduplicate by field name (rtl_loop effectively de-dupes via state_topic/unique_id).
    seen = set()
    deduped: list[tuple[str, object]] = []
    for field, value in planned:
        if field in seen:
            continue
        seen.add(field)
        deduped.append((field, value))
    return deduped


def test_field_meta_covers_published_fields_from_captured_fixtures():
    fixtures_dir = Path(__file__).parent / "fixtures" / "rtl433"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    # Enable this test by dropping a JSON fixture file in tests/fixtures/rtl433.
    candidates = (
        list(fixtures_dir.glob("*.jsonl"))
        + list(fixtures_dir.glob("*.ndjson"))
        + list(fixtures_dir.glob("*.json"))
    )
    if not candidates:
        pytest.skip(f"No rtl_433 JSON fixtures found in {fixtures_dir} (*.jsonl/*.ndjson/*.json)")

    missing: dict[str, set[str]] = {}

    for path, obj in _iter_fixture_events(fixtures_dir):
        model = str(obj.get("model") or obj.get("device") or "Unknown")
        planned = _planned_publish_fields(data_raw=obj, model=model)

        for field, value in planned:
            # Skip array-derived keys by default (often raw payload bytes, codes, etc.)
            if _ARRAY_DERIVED_KEY_RE.match(field):
                continue

            # rtl_loop never dispatches None values.
            if value is None:
                continue

            # Use model-specific overrides when available.
            meta = get_field_meta(field, model, base_meta=FIELD_META)
            if meta is None:
                missing.setdefault(path.name, set()).add(field)

    if missing:
        lines = ["Missing FIELD_META entries for published fields found in fixtures:"]
        for fname in sorted(missing):
            fields = sorted(missing[fname])
            lines.append(f"- {fname}: {', '.join(fields)}")
        lines.append("")
        lines.append("Paste-ready stubs:")
        for fname in sorted(missing):
            for f in sorted(missing[fname]):
                friendly = f.replace("_", " ").strip().title().replace('"', "'")
                lines.append(f'    "{f}": (None, "none", "mdi:eye", "{friendly}"),')
        raise AssertionError("\n".join(lines))
