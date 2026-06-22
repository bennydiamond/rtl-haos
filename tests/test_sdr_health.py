# tests/test_sdr_health.py
"""Tests for SDR health monitoring."""
import time

import pytest

import config
import sdr_health
from sdr_health import SDRHealthMonitor, get_health_monitor


@pytest.fixture(autouse=True)
def reset_health_singleton():
    """Reset the singleton and its state before each test."""
    sdr_health._health_monitor = None
    SDRHealthMonitor._instance = None
    yield
    # Cleanup after test
    sdr_health._health_monitor = None
    SDRHealthMonitor._instance = None


@pytest.fixture
def health():
    """Get a fresh health monitor instance."""
    return get_health_monitor()


class TestSDRHealthMonitorSingleton:
    """Test singleton behavior of SDRHealthMonitor."""

    def test_singleton_returns_same_instance(self):
        """Multiple calls to get_health_monitor return the same instance."""
        h1 = get_health_monitor()
        h2 = get_health_monitor()
        assert h1 is h2

    def test_direct_instantiation_returns_singleton(self):
        """Direct instantiation also returns the singleton."""
        h1 = SDRHealthMonitor()
        h2 = SDRHealthMonitor()
        assert h1 is h2


class TestRestartLoopDetection:
    """Test restart loop detection."""

    def test_no_restarts_is_healthy(self, health, monkeypatch):
        """No restarts means healthy."""
        is_problem, reason = health.check_health()
        assert is_problem is False
        assert reason == ""

    def test_single_restart_not_a_problem(self, health, monkeypatch):
        """A single restart should not trigger an alert."""
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 3, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_WINDOW", 600, raising=False)

        health.record_restart("Radio1")

        is_problem, reason = health.check_health()
        assert is_problem is False
        assert reason == ""

    def test_restart_loop_triggers_alert(self, health, monkeypatch):
        """Multiple restarts within window trigger an alert."""
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 3, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_WINDOW", 600, raising=False)

        # Record 3 restarts
        health.record_restart("Radio1")
        health.record_restart("Radio1")
        health.record_restart("Radio1")

        is_problem, reason = health.check_health()
        assert is_problem is True
        assert "Radio1" in reason
        assert "restart loop" in reason

    def test_restarts_outside_window_not_counted(self, health, monkeypatch):
        """Restarts outside the window should be pruned."""
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 3, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_WINDOW", 600, raising=False)

        # Manually insert old timestamps
        old_time = time.time() - 700  # Outside window
        health.restart_times["Radio1"] = [old_time, old_time + 1, old_time + 2]

        # Record one new restart
        health.record_restart("Radio1")

        is_problem, reason = health.check_health()
        # Only 1 restart in window, should be healthy
        assert is_problem is False


class TestZeroDataDetection:
    """Test zero-data detection."""

    def test_no_data_ever_is_healthy(self, health, monkeypatch):
        """If we've never seen data, no timeout alert (no radios known)."""
        monkeypatch.setattr(config, "SDR_HEALTH_NO_DATA_TIMEOUT", 900, raising=False)

        is_problem, reason = health.check_health()
        assert is_problem is False

    def test_recent_data_is_healthy(self, health, monkeypatch):
        """Recent data within timeout is healthy."""
        monkeypatch.setattr(config, "SDR_HEALTH_NO_DATA_TIMEOUT", 900, raising=False)

        health.record_data_received("Radio1")

        is_problem, reason = health.check_health()
        assert is_problem is False

    def test_stale_data_triggers_alert(self, health, monkeypatch):
        """No data for longer than timeout triggers alert."""
        monkeypatch.setattr(config, "SDR_HEALTH_NO_DATA_TIMEOUT", 900, raising=False)

        # Set last data time to be old
        old_time = time.time() - 1000  # Older than timeout
        health.last_data_time["Radio1"] = old_time

        is_problem, reason = health.check_health()
        assert is_problem is True
        assert "Radio1" in reason
        assert "no data" in reason


class TestErrorRecording:
    """Test error recording and clearing."""

    def test_record_error_triggers_alert(self, health):
        """Recording an error triggers an alert."""
        health.record_error("Radio1", "USB disconnected")

        is_problem, reason = health.check_health()
        assert is_problem is True
        assert "Radio1" in reason
        assert "USB disconnected" in reason

    def test_clear_error_removes_alert(self, health):
        """Clearing an error removes the alert."""
        health.record_error("Radio1", "USB disconnected")
        health.clear_error("Radio1")

        is_problem, reason = health.check_health()
        assert is_problem is False
        assert reason == ""

    def test_clear_error_nonexistent_is_safe(self, health):
        """Clearing a non-existent error is safe (no error)."""
        health.clear_error("NonExistent")
        is_problem, reason = health.check_health()
        assert is_problem is False


class TestMultipleConditions:
    """Test multiple health conditions combined."""

    def test_multiple_radios_with_errors(self, health):
        """Multiple radios can have separate errors."""
        health.record_error("Radio1", "USB disconnected")
        health.record_error("Radio2", "Permission denied")

        is_problem, reason = health.check_health()
        assert is_problem is True
        assert "Radio1" in reason
        assert "Radio2" in reason
        assert "USB disconnected" in reason
        assert "Permission denied" in reason

    def test_combined_conditions(self, health, monkeypatch):
        """Multiple condition types combine into one reason."""
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 2, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_WINDOW", 600, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_NO_DATA_TIMEOUT", 900, raising=False)

        # Restart loop for Radio1
        health.record_restart("Radio1")
        health.record_restart("Radio1")

        # Error for Radio2
        health.record_error("Radio2", "USB busy")

        is_problem, reason = health.check_health()
        assert is_problem is True
        assert "restart loop" in reason
        assert "USB busy" in reason


class TestReset:
    """Test the reset functionality."""

    def test_reset_clears_all_state(self, health, monkeypatch):
        """reset() clears all tracked state."""
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 2, raising=False)
        monkeypatch.setattr(config, "SDR_HEALTH_RESTART_WINDOW", 600, raising=False)

        health.record_restart("Radio1")
        health.record_restart("Radio1")
        health.record_error("Radio2", "Error")
        health.record_data_received("Radio3")

        # Should have issues
        is_problem, _ = health.check_health()
        assert is_problem is True

        # Reset
        health.reset()

        # Should be healthy
        is_problem, reason = health.check_health()
        assert is_problem is False
        assert reason == ""
        assert len(health.restart_times) == 0
        assert len(health.last_data_time) == 0
        assert len(health.current_errors) == 0


class TestGetAllRadios:
    """Test get_all_radios method."""

    def test_get_all_radios_empty(self, health):
        """Initially returns empty set."""
        radios = health.get_all_radios()
        assert radios == set()

    def test_get_all_radios_from_various_sources(self, health):
        """Returns radios from all tracking dicts."""
        health.record_restart("Radio1")
        health.record_data_received("Radio2")
        health.record_error("Radio3", "Error")

        radios = health.get_all_radios()
        assert radios == {"Radio1", "Radio2", "Radio3"}
