"""
Integration tests that use the external `rtl_433` binary.

These tests are *opt-in* and will be skipped unless explicitly enabled.
They are intended for local / self-hosted runners, NOT public CI by default.

Enable integration tests:
  RUN_RTL433_TESTS=1 pytest -m integration

Enable hardware smoke tests (requires an RTL-SDR device):
  RUN_HARDWARE_TESTS=1 pytest -m hardware
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest


def _require_env(name: str) -> None:
    if os.getenv(name) != "1":
        pytest.skip(f"Set {name}=1 to enable this test")


def _require_rtl433() -> str:
    exe = shutil.which("rtl_433")
    if not exe:
        pytest.skip("rtl_433 is not installed or not in PATH")
    return exe


@pytest.mark.integration
def test_rtl433_binary_runs_help() -> None:
    """
    Basic smoke test: ensure `rtl_433` is runnable.

    Uses `-h` so it does not require hardware.
    """
    _require_env("RUN_RTL433_TESTS")
    exe = _require_rtl433()

    proc = subprocess.run(
        [exe, "-h"],
        capture_output=True,
        text=True,
        timeout=10,
    )

    # Most builds exit 0 for -h; if not, show output for debugging.
    assert proc.returncode == 0, (
        f"rtl_433 -h failed\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    out = (proc.stdout + "\n" + proc.stderr).lower()
    assert "rtl_433" in out or "usage" in out


@pytest.mark.integration
def test_rtl433_replay_fixture_emits_json() -> None:
    """
    Replay-mode integration test.

    If you place a recording in tests/fixtures/rtl433/, this test will run rtl_433
    against it and assert that at least one JSON event is produced.

    Supported fixture extensions vary by rtl_433 build; common ones include:
      - .cu8  (complex unsigned 8-bit I/Q)
      - .cs8  (complex signed 8-bit I/Q)
      - .wav  (audio/wav demod recordings)

    If no fixture exists, the test is skipped.
    """
    _require_env("RUN_RTL433_TESTS")
    exe = _require_rtl433()

    fixtures_dir = Path(__file__).parent / "fixtures" / "rtl433"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    candidates = []
    for ext in ("*.cu8", "*.cs8", "*.wav", "*.cfile", "*.iq"):
        candidates.extend(sorted(fixtures_dir.glob(ext)))

    if not candidates:
        pytest.skip(f"No rtl_433 replay fixture found in {fixtures_dir}")

    fixture = str(candidates[0])

    # -F json: JSON lines on stdout
    # NOTE: rtl_433 will usually run to EOF for -r inputs; we still set a timeout.
    proc = subprocess.run(
        [exe, "-r", fixture, "-F", "json"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    # Some fixtures might produce warnings to stderr; don't require stderr to be empty.
    assert proc.returncode == 0, (
        f"rtl_433 replay failed\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )

    json_lines = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            json_lines.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    assert json_lines, "No JSON events produced by rtl_433 replay fixture"
    # Sanity check: most rtl_433 JSON objects include at least a model/device identifier
    assert any(("model" in obj) or ("device" in obj) for obj in json_lines)


@pytest.mark.hardware
def test_rtl433_live_smoke_short_run() -> None:
    """
    Live hardware smoke test.

    Runs rtl_433 for a couple seconds (-T) and asserts that it starts and exits
    cleanly. This test does NOT require receiving any RF events.

    If no RTL-SDR device is present, the test is skipped (not failed) to keep it
    friendly for occasional local runs.
    """
    _require_env("RUN_HARDWARE_TESTS")
    exe = _require_rtl433()

    proc = subprocess.run(
        [exe, "-T", "2", "-F", "json"],
        capture_output=True,
        text=True,
        timeout=15,
    )

    combined = (proc.stdout + "\n" + proc.stderr).lower()

    # Common "no dongle / no perms" messages: skip rather than fail
    if proc.returncode != 0:
        if "no supported devices found" in combined:
            pytest.skip("No RTL-SDR device detected (rtl_433 reported none found)")
        if "usb_open error" in combined or "permission denied" in combined:
            pytest.skip("RTL-SDR present but not accessible (permissions/USB open error)")
        assert False, f"rtl_433 live run failed\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"

    # If it ran successfully, great. No assertion about RF activity.
    assert True
