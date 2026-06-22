# sdr_health.py
"""
SDR Health Monitoring for rtl-haos.

Tracks SDR health conditions and provides a binary health status for Home Assistant.
Alert conditions:
  - Crash/restart loop: 3+ restarts within 10 minutes (configurable)
  - Zero data: No sensor readings received in 15 minutes (configurable)
  - USB errors: Device disconnected, busy, or permission denied
  - rtl_433 crash: Segfault, illegal instruction
"""
from __future__ import annotations

import threading
import time
from typing import Optional

import config


class SDRHealthMonitor:
    """Singleton health monitor for SDR radios."""

    _instance: Optional["SDRHealthMonitor"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "SDRHealthMonitor":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        if self._initialized:
            return
        self._initialized = True

        # Track restart timestamps per radio for loop detection
        self.restart_times: dict[str, list[float]] = {}

        # Track last data received timestamp per radio
        self.last_data_time: dict[str, float] = {}

        # Track current error state per radio (None = no error)
        self.current_errors: dict[str, str] = {}

        # Overall alert state (cached for efficiency)
        self._alert_state: bool = False
        self._alert_reason: str = ""

        # Lock for thread-safe access
        self._state_lock = threading.Lock()

    def record_restart(self, radio_name: str) -> None:
        """Record a radio restart for loop detection."""
        now = time.time()
        with self._state_lock:
            if radio_name not in self.restart_times:
                self.restart_times[radio_name] = []
            self.restart_times[radio_name].append(now)

            # Prune old timestamps outside the window
            window = getattr(config, "SDR_HEALTH_RESTART_WINDOW", 600)
            cutoff = now - window
            self.restart_times[radio_name] = [
                t for t in self.restart_times[radio_name] if t > cutoff
            ]

    def record_data_received(self, radio_name: str) -> None:
        """Record that valid data was received from a radio."""
        now = time.time()
        with self._state_lock:
            self.last_data_time[radio_name] = now

    def record_error(self, radio_name: str, error_type: str) -> None:
        """Record an error condition for a radio."""
        with self._state_lock:
            self.current_errors[radio_name] = error_type

    def clear_error(self, radio_name: str) -> None:
        """Clear error state for a radio (e.g., when it comes back online)."""
        with self._state_lock:
            self.current_errors.pop(radio_name, None)

    def check_health(self) -> tuple[bool, str]:
        """Check overall SDR health.

        Returns:
            tuple[bool, str]: (is_problem, reason)
                - is_problem: True if there's a health problem
                - reason: Human-readable reason for the problem, or empty if healthy
        """
        now = time.time()
        problems: list[str] = []

        with self._state_lock:
            # Check for restart loops
            threshold = getattr(config, "SDR_HEALTH_RESTART_THRESHOLD", 3)
            window = getattr(config, "SDR_HEALTH_RESTART_WINDOW", 600)
            cutoff = now - window

            for radio_name, timestamps in self.restart_times.items():
                # Filter to recent timestamps
                recent = [t for t in timestamps if t > cutoff]
                self.restart_times[radio_name] = recent

                if len(recent) >= threshold:
                    problems.append(f"{radio_name}: restart loop ({len(recent)}x in {window}s)")

            # Check for zero data (only if we've seen data before)
            timeout = getattr(config, "SDR_HEALTH_NO_DATA_TIMEOUT", 900)
            for radio_name, last_time in self.last_data_time.items():
                if (now - last_time) > timeout:
                    minutes = int((now - last_time) / 60)
                    problems.append(f"{radio_name}: no data ({minutes}m)")

            # Check for current errors
            for radio_name, error_type in self.current_errors.items():
                problems.append(f"{radio_name}: {error_type}")

        if problems:
            # Combine all problems into a single reason string
            reason = "; ".join(problems)
            self._alert_state = True
            self._alert_reason = reason
            return (True, reason)

        self._alert_state = False
        self._alert_reason = ""
        return (False, "")

    def get_all_radios(self) -> set[str]:
        """Get the set of all known radio names."""
        with self._state_lock:
            radios = set(self.restart_times.keys())
            radios.update(self.last_data_time.keys())
            radios.update(self.current_errors.keys())
            return radios

    def reset(self) -> None:
        """Reset all health state (useful for testing)."""
        with self._state_lock:
            self.restart_times.clear()
            self.last_data_time.clear()
            self.current_errors.clear()
            self._alert_state = False
            self._alert_reason = ""


# Module-level singleton instance
_health_monitor: Optional[SDRHealthMonitor] = None


def get_health_monitor() -> SDRHealthMonitor:
    """Get the singleton health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = SDRHealthMonitor()
    return _health_monitor
