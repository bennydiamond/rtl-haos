#!/usr/bin/env python3
"""
FILE: system_monitor.py
DESCRIPTION:
  A threaded loop that gathers local system statistics.
  - UPDATED: Now publishes Config lists (Blacklist, Whitelist, Main Sensors)
    to the Diagnostic tab.
"""
import time
import threading
import sys
import importlib.util
import socket
import subprocess
# --- IMPORTS & DEPENDENCY CHECK ---
# NOTE: During unit tests, conftest may inject a dummy "psutil" into sys.modules.
# Some mocks (e.g. MagicMock) don't provide __spec__, which can cause
# importlib.util.find_spec("psutil") to raise ValueError. Treat that the same
# as "psutil not available".
PSUTIL_AVAILABLE = False
try:
    try:
        psutil_spec = importlib.util.find_spec("psutil")
    except ValueError:
        psutil_spec = None

    if psutil_spec is not None:
        import psutil  # noqa: F401
        from sensors_system import SystemMonitor
        PSUTIL_AVAILABLE = True
    else:
        print("[WARN] 'psutil' not found. CPU/RAM stats will be disabled, but Device List will work.")
except ImportError as e:
    PSUTIL_AVAILABLE = False
    print(f"[WARN] System Monitoring disabled: {e}")


# Safe imports for the rest of the app
import config
from mqtt_handler import HomeNodeMQTT
from utils import get_system_mac 

def format_list_for_ha(data_list):
    """Joins a list into a string and truncates to ~250 chars."""
    if not data_list:
        return "None"
    
    # Convert all items to string just in case
    str_list = [str(i) for i in data_list]
    joined = ", ".join(sorted(str_list))
    
    if len(joined) > 250:
        return joined[:247] + "..."
    return joined


# Cache rtl_433 version so we don't spawn a process every minute.
_RTL_433_VERSION_CACHE = None

def _get_rtl_433_version():
    """Return a short rtl_433 version string (best-effort)."""
    bin_path = getattr(config, "RTL_433_BIN", None) or "rtl_433"
    try:
        proc = subprocess.run(
            [bin_path, "-V"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5,
            check=False,
        )
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        for line in combined.splitlines():
            line = line.strip()
            if line:
                return line[:250]
        return "Unknown"
    except FileNotFoundError:
        return "rtl_433 not found"
    except Exception as e:
        return f"Unknown ({type(e).__name__})"

def get_rtl_433_version_cached():
    global _RTL_433_VERSION_CACHE
    if _RTL_433_VERSION_CACHE is None:
        _RTL_433_VERSION_CACHE = _get_rtl_433_version()
    return _RTL_433_VERSION_CACHE

def system_stats_loop(mqtt_handler, DEVICE_ID, MODEL_NAME):
    
    # Initialize Hardware Monitor if available
    sys_mon = None
    if PSUTIL_AVAILABLE:
        try:
            sys_mon = SystemMonitor()
            print("[STARTUP] Hardware Monitor (psutil) initialized.")
        except Exception as e:
            print(f"[WARN] Hardware Monitor failed to start: {e}")

    print("[STARTUP] Starting System Monitor Loop...")


    start_time = time.monotonic()
    rtl_433_version = get_rtl_433_version_cached()
    
    while True:
        device_name = f"{MODEL_NAME} ({DEVICE_ID})" 

        # --- 1. BRIDGE METRICS (Always Run) ---
        try:
            # A. Tracked Devices
            # devices = mqtt_handler.tracked_devices
            # count = len(devices)
            # dev_list_str = format_list_for_ha(devices) if count > 0 else "Scanning..."

            mqtt_handler.send_sensor(DEVICE_ID, "sys_rtl_433_version", rtl_433_version, device_name, MODEL_NAME, is_rtl=True)
            # mqtt_handler.send_sensor(DEVICE_ID, "sys_device_list", dev_list_str, device_name, MODEL_NAME, is_rtl=True)

            uptime_s = int(time.monotonic() - start_time)
            mqtt_handler.send_sensor(DEVICE_ID, "sys_bridge_uptime", uptime_s, device_name, MODEL_NAME, is_rtl=True)

            # B. Configuration Lists (Sent as Diagnostics)
            # We fetch these fresh from config every loop in case of future hot-reloads
            # bl = getattr(config, "DEVICE_BLACKLIST", [])
            # wl = getattr(config, "DEVICE_WHITELIST", [])
            # ms = getattr(config, "MAIN_SENSORS", [])

            # mqtt_handler.send_sensor(DEVICE_ID, "sys_cfg_blacklist", format_list_for_ha(bl), device_name, MODEL_NAME, is_rtl=True)
            # mqtt_handler.send_sensor(DEVICE_ID, "sys_cfg_whitelist", format_list_for_ha(wl), device_name, MODEL_NAME, is_rtl=True)
            # mqtt_handler.send_sensor(DEVICE_ID, "sys_cfg_sensors", format_list_for_ha(ms), device_name, MODEL_NAME, is_rtl=True)
            
        except Exception as e:
            print(f"[ERROR] Bridge Stats update failed: {e}")

        # --- 2. HARDWARE METRICS (Only if psutil is working) ---
        if sys_mon:
            try:
                stats = sys_mon.read_stats()
                for key, value in stats.items(): 
                    mqtt_handler.send_sensor(
                        DEVICE_ID, 
                        key, 
                        value, 
                        device_name, 
                        MODEL_NAME, 
                        is_rtl=True 
                    )
            except Exception as e:
                print(f"[SYSTEM ERROR] Hardware stats failed: {e}")
            
        time.sleep(60) 

if __name__ == "__main__":
    BASE_DEVICE_ID = get_system_mac().replace(":","").lower()
    BASE_MODEL_NAME = config.BRIDGE_NAME

    print("--- SYSTEM MONITOR STARTING ---")

    mqtt_handler = HomeNodeMQTT()
    mqtt_handler.start()

    mqtt_handler.device_count_channel.start_thread(
        BASE_DEVICE_ID,
        BASE_MODEL_NAME,
        thread_factory=threading.Thread,
    )

    try:
        thread = threading.Thread(
            target=system_stats_loop,
            args=(mqtt_handler, BASE_DEVICE_ID, BASE_MODEL_NAME),
            name="system_stats_loop",
            daemon=True,
        )
    except TypeError:
        thread = threading.Thread(
            target=system_stats_loop,
            args=(mqtt_handler, BASE_DEVICE_ID, BASE_MODEL_NAME),
            daemon=True,
        )
    thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[SHUTDOWN] Stopping MQTT...")
        mqtt_handler.stop()
