"""Hardware-gated smoke tests for the add-on `/share` mapping.

This test is intended to run *inside* the Home Assistant add-on container,
where the Supervisor bind-mounts the host share folder at `/share` when the
add-on manifest includes `map: - share`.

Why you might see a skip locally:
- When running pytest in a normal venv on your workstation, there is no
  Supervisor, so `/share` typically won't exist.
- The static test `tests/test_addon_share_mapping.py` covers the manifest
  regression in CI; this runtime smoke test is for in-container verification.

Enable:
  RUN_HARDWARE_TESTS=1

Force expectation outside HAOS (rare, e.g. local docker bind-mount):
  RUN_HARDWARE_TESTS=1 EXPECT_SHARE_MOUNT=1

Optional: assert a specific file is readable from /share:
  RUN_HARDWARE_TESTS=1 RTL_HAOS_SHARE_TEST_FILE=rtl_433.conf
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _require_env(name: str) -> None:
    if os.getenv(name) != "1":
        pytest.skip(f"Set {name}=1 to enable this test")


def _running_in_ha_addon_container() -> bool:
    """Best-effort detection that we're inside a HA add-on container."""
    if os.getenv("SUPERVISOR_TOKEN") or os.getenv("HASSIO_TOKEN"):
        return True
    # Common add-on filesystem markers
    if Path("/data/options.json").exists():
        return True
    return False


def _should_expect_share_mount() -> bool:
    # Allow developers to force this expectation in custom environments.
    if os.getenv("EXPECT_SHARE_MOUNT") == "1":
        return True
    return _running_in_ha_addon_container()


@pytest.mark.hardware
def test_share_mount_present_and_readable():
    _require_env("RUN_HARDWARE_TESTS")

    share = Path("/share")

    # If we're not in an add-on container, don't fail just because /share
    # isn't present (that's normal on a workstation).
    if not _should_expect_share_mount() and not share.exists():
        pytest.skip("Not running inside HA add-on container; /share mount not expected")

    assert share.is_dir(), "Expected /share to be mounted in the add-on container"

    # Must be readable to support config files referenced from /share.
    assert os.access(str(share), os.R_OK), "Expected /share to be readable"

    # Optional: verify a user-provided file exists and is readable.
    provided = os.getenv("RTL_HAOS_SHARE_TEST_FILE", "").strip()
    if provided:
        p = Path(provided)
        if not p.is_absolute():
            p = share / provided

        # Safety: only allow asserting files under /share
        try:
            p_abs = p.resolve()
        except FileNotFoundError:
            p_abs = p

        assert str(p_abs).startswith(str(share)), (
            f"RTL_HAOS_SHARE_TEST_FILE must point to a file under /share (got: {p})"
        )
        assert p.exists(), f"Expected share test file to exist: {p}"
        assert os.access(str(p), os.R_OK), f"Expected share test file to be readable: {p}"
        _ = p.read_text(encoding="utf-8", errors="ignore")

    # If /share is writable, round-trip a temp file.
    if os.access(str(share), os.W_OK):
        tmp = share / f".rtl_haos_share_mount_test_{os.getpid()}.txt"
        try:
            tmp.write_text("rtl-haos share mount test\n", encoding="utf-8")
            assert tmp.read_text(encoding="utf-8").startswith("rtl-haos share mount test")
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                # If unlink fails due to perms, don't fail the test; the mount itself is what matters.
                pass
