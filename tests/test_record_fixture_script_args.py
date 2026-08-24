from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def run_script(*args: str) -> subprocess.CompletedProcess[str]:
    repo_root = Path(__file__).resolve().parent.parent
    script = repo_root / "scripts" / "record_rtl433_fixture.sh"
    assert script.exists()
    return subprocess.run(
        ["bash", str(script), "--dry-run", *args],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        timeout=10,
    )


def test_accepts_suffixes() -> None:
    proc = run_script("433.92M", "250k", "10", "tests/fixtures/rtl433/x.cu8")
    assert proc.returncode == 0, proc.stderr
    out = proc.stdout
    assert "freq_hz:" in out and "433920000" in out
    assert "rate_hz:" in out and "250000" in out


def test_rejects_float_without_unit_suffix_freq() -> None:
    proc = run_script("433.92", "250k", "10", "tests/fixtures/rtl433/x.cu8")
    assert proc.returncode != 0
    assert "ambiguous" in (proc.stderr or "").lower()
    assert "433.92m" in (proc.stderr or "").lower() or "use e.g." in (proc.stderr or "").lower()


def test_rejects_plain_int_rate_that_is_likely_missing_k() -> None:
    # sample rate "250" is almost certainly a mistake; our script treats it as invalid
    proc = run_script("433.92M", "250", "10", "tests/fixtures/rtl433/x.cu8")
    assert proc.returncode != 0
    err = (proc.stderr or "").lower()
    assert "too small" in err or "did you mean" in err or "ambiguous" in err


def test_style_b_outfile_seconds_works() -> None:
    proc = run_script("tests/fixtures/rtl433/x.cu8", "10", "433.92M", "250k")
    assert proc.returncode == 0, proc.stderr
