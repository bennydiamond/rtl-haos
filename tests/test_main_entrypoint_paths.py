"""Coverage-focused tests for main.py.

These tests exercise the branching logic in main.main() (manual config,
auto-multi, and fallback modes) while stubbing out threads/sleeps/subprocesses.

Importing main.py replaces builtins.print globally; each test restores it.
"""

from __future__ import annotations

import builtins

import pytest


class DummyThread:
    """Thread stub that records starts without running targets."""

    created = []

    def __init__(self, target=None, args=(), kwargs=None, daemon=None):
        self.target = target
        self.args = args
        self.kwargs = kwargs or {}
        self.daemon = daemon
        self.started = False
        DummyThread.created.append(self)

    def start(self):
        self.started = True


class DummyMQTT:
    instances = []

    def __init__(self, version="Unknown", *args, **kwargs):
        self.version = version
        self.started = False
        self.stopped = False
        self.device_count_channel = type(
            "_Ch",
            (),
            {
                "loop": lambda self, *a, **k: None,
                "start_thread": lambda self, *a, **k: None,
            },
        )()
        DummyMQTT.instances.append(self)

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    # rtl_manager expects mqtt_handler to have send_sensor, but we never call it here.
    def send_sensor(self, *_args, **_kwargs):
        return None

    def _get_discovery_enabled(self):
        return True

    def cleanup_device_discovered_topics(self, clean_id):
        pass

    def publish_known_devices_select(self):
        pass


class DummyProcessor:
    def __init__(self, mqtt_handler, *args, **kwargs):
        self.mqtt = mqtt_handler

    def start_throttle_loop(self):
        return None


def _patch_sleep_to_exit(monkeypatch, main_mod):
    """Exit main's infinite loop by raising KeyboardInterrupt on the 1s sleep."""

    def fake_sleep(secs):
        if secs == 1:
            raise KeyboardInterrupt()
        return None

    monkeypatch.setattr(main_mod.time, "sleep", fake_sleep)


@pytest.fixture
def main_mod():
    orig_print = builtins.print
    import main as m
    try:
        yield m
    finally:
        builtins.print = orig_print
        DummyThread.created.clear()
        DummyMQTT.instances.clear()


def test_main_manual_config_duplicate_ids_and_unconfigured(monkeypatch, main_mod):
    # --- Patch external side effects ---
    monkeypatch.setattr(main_mod, "check_dependencies", lambda: None)
    monkeypatch.setattr(main_mod, "get_version", lambda: "vTest")
    monkeypatch.setattr(main_mod, "show_logo", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "HomeNodeMQTT", DummyMQTT)
    monkeypatch.setattr(main_mod, "DataProcessor", DummyProcessor)
    monkeypatch.setattr(main_mod.threading, "Thread", DummyThread)
    monkeypatch.setattr(main_mod, "rtl_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "system_stats_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "get_system_mac", lambda: "aa:bb:cc:dd:ee:ff")

    # Exercise warnings path from validate_radio_config
    monkeypatch.setattr(main_mod, "validate_radio_config", lambda _radio: ["warn"])

    # Physical device scan
    monkeypatch.setattr(
        main_mod,
        "discover_rtl_devices",
        lambda: [
            {"name": "RTL_101", "id": "101", "index": 0},
            {"name": "RTL_102", "id": "102", "index": 1},
        ],
    )

    cfg = main_mod.config
    monkeypatch.setattr(cfg, "BRIDGE_NAME", "Bridge", raising=False)

    # Manual config includes:
    # - a matching serial (101)
    # - a duplicate serial (101) that should be skipped
    # - a missing serial (999) that should warn but still start
    monkeypatch.setattr(
        cfg,
        "RTL_CONFIG",
        [
            {"name": "A", "id": "101", "freq": "915M", "rate": "1024k", "hop_interval": 0},
            {"name": "Dup", "id": "101", "freq": "915M", "rate": "1024k", "hop_interval": 0},
            {"name": "Missing", "id": "999", "freq": "915M", "rate": "1024k", "hop_interval": 0},
        ],
        raising=False,
    )

    _patch_sleep_to_exit(monkeypatch, main_mod)

    # Run
    main_mod.main()

    # Two rtl_loop threads should be started: A + Missing (Dup skipped)
    rtl_threads = [t for t in DummyThread.created if t.target == main_mod.rtl_loop]
    assert len(rtl_threads) == 2

    # First configured radio should be matched to index 0
    radio_a = rtl_threads[0].args[0]
    assert radio_a.get("id") == "101"
    assert radio_a.get("index") == 0

    # Ensure graceful shutdown called stop()
    assert DummyMQTT.instances and DummyMQTT.instances[-1].stopped is True


def test_main_auto_multi_three_radios_split_secondary(monkeypatch, main_mod):
    monkeypatch.setattr(main_mod, "check_dependencies", lambda: None)
    monkeypatch.setattr(main_mod, "get_version", lambda: "vTest")
    monkeypatch.setattr(main_mod, "show_logo", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "HomeNodeMQTT", DummyMQTT)
    monkeypatch.setattr(main_mod, "DataProcessor", DummyProcessor)
    monkeypatch.setattr(main_mod.threading, "Thread", DummyThread)
    monkeypatch.setattr(main_mod, "rtl_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "system_stats_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "get_system_mac", lambda: "aa:bb:cc:dd:ee:ff")

    # Scan shows 3 radios
    monkeypatch.setattr(
        main_mod,
        "discover_rtl_devices",
        lambda: [
            {"name": "RTL_101", "id": "101", "index": 0},
            {"name": "RTL_102", "id": "102", "index": 1},
            {"name": "RTL_103", "id": "103", "index": 2},
        ],
    )

    # Ensure we exercise the "US known" branch and the 2-freq split path.
    monkeypatch.setattr(main_mod, "get_homeassistant_country_code", lambda: "US")
    monkeypatch.setattr(main_mod, "choose_secondary_band_defaults", lambda **_k: ("868M,915M", 15))

    # Produce warnings in the validate loop
    monkeypatch.setattr(main_mod, "validate_radio_config", lambda _radio: ["warn"])

    cfg = main_mod.config
    monkeypatch.setattr(cfg, "BRIDGE_NAME", "Bridge", raising=False)

    # Unconfigured (auto) mode
    monkeypatch.setattr(cfg, "RTL_CONFIG", None, raising=False)

    # Enable auto multi
    monkeypatch.setattr(cfg, "RTL_AUTO_MULTI", True, raising=False)
    monkeypatch.setattr(cfg, "RTL_AUTO_MAX_RADIOS", 0, raising=False)
    monkeypatch.setattr(cfg, "RTL_AUTO_HARD_CAP", 3, raising=False)
    monkeypatch.setattr(cfg, "RTL_AUTO_BAND_PLAN", "auto", raising=False)

    # Force primary to have >1 freq and hop interval <=0 so it auto-defaults to 60.
    monkeypatch.setattr(cfg, "RTL_DEFAULT_FREQ", "433.92M,315M", raising=False)
    monkeypatch.setattr(cfg, "RTL_DEFAULT_HOP_INTERVAL", 0, raising=False)
    monkeypatch.setattr(cfg, "RTL_DEFAULT_RATE", "1024k", raising=False)

    _patch_sleep_to_exit(monkeypatch, main_mod)

    main_mod.main()

    rtl_threads = [t for t in DummyThread.created if t.target == main_mod.rtl_loop]
    assert len(rtl_threads) == 3

    radio1 = rtl_threads[0].args[0]
    radio2 = rtl_threads[1].args[0]
    radio3 = rtl_threads[2].args[0]

    assert radio1.get("slot") == 0
    assert radio1.get("hop_interval") == 60  # auto default when multiple freqs + hop<=0

    assert radio2.get("slot") == 1
    assert str(radio2.get("freq")).startswith("868")

    assert radio3.get("slot") == 2
    assert str(radio3.get("freq")).startswith("915")


def test_main_no_hardware_fallback_starts_device_0(monkeypatch, main_mod):
    monkeypatch.setattr(main_mod, "check_dependencies", lambda: None)
    monkeypatch.setattr(main_mod, "get_version", lambda: "vTest")
    monkeypatch.setattr(main_mod, "show_logo", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "HomeNodeMQTT", DummyMQTT)
    monkeypatch.setattr(main_mod, "DataProcessor", DummyProcessor)
    monkeypatch.setattr(main_mod.threading, "Thread", DummyThread)
    monkeypatch.setattr(main_mod, "rtl_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "system_stats_loop", lambda *_a, **_k: None)
    monkeypatch.setattr(main_mod, "get_system_mac", lambda: "aa:bb:cc:dd:ee:ff")
    monkeypatch.setattr(main_mod, "validate_radio_config", lambda _radio: [])

    # No devices detected
    monkeypatch.setattr(main_mod, "discover_rtl_devices", lambda: [])

    cfg = main_mod.config
    monkeypatch.setattr(cfg, "BRIDGE_NAME", "Bridge", raising=False)
    monkeypatch.setattr(cfg, "RTL_CONFIG", None, raising=False)

    # Fallback should disable hopping when only one freq is set.
    monkeypatch.setattr(cfg, "RTL_DEFAULT_FREQ", "915M", raising=False)
    monkeypatch.setattr(cfg, "RTL_DEFAULT_HOP_INTERVAL", 30, raising=False)
    monkeypatch.setattr(cfg, "RTL_DEFAULT_RATE", "1024k", raising=False)

    _patch_sleep_to_exit(monkeypatch, main_mod)

    main_mod.main()

    rtl_threads = [t for t in DummyThread.created if t.target == main_mod.rtl_loop]
    assert len(rtl_threads) == 1
    auto_radio = rtl_threads[0].args[0]
    assert auto_radio.get("id") == "0"
    assert auto_radio.get("hop_interval") == 0
