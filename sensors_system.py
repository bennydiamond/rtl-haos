"""
FILE: sensors_system.py
DESCRIPTION:
  The low-level class that interfaces with the Operating System.
  - Uses the 'psutil' library to read CPU load, Memory usage, and Disk space.
  - Reads Linux thermal zones for CPU temperature.
"""

import shutil
import time
import os
import platform
import socket

# We don't wrap this in try/except because system_monitor.py
# guarantees this file is only imported if psutil exists.
import psutil


class SystemMonitor:
    def __init__(self):
        self.boot_time = psutil.boot_time()
        # Get the current process (this script) to track its specific RAM usage
        self.process = psutil.Process(os.getpid())

        # --- STATIC INFO (Read once at startup) ---
        self.os_info = f"{platform.system()} {platform.release()}"

        # Try to get the actual Raspberry Pi model name
        try:
            with open("/sys/firmware/devicetree/base/model", "r") as f:
                # Remove null characters often found in this file
                self.model_info = f.read().strip().strip("\x00")
        except FileNotFoundError:
            self.model_info = socket.gethostname()

    def read_stats(self):
        stats = {}

        # 1. CPU (Usually always available)
        try:
            stats["sys_cpu"] = psutil.cpu_percent(interval=1)
        except:
            pass

        # 2. Memory (Total System RAM %)
        try:
            svmem = psutil.virtual_memory()
            stats["sys_mem"] = svmem.percent
        except:
            pass

        # 3. Script RAM Usage (MB)
        try:
            script_mem_bytes = self.process.memory_info().rss
            stats["sys_script_mem"] = round(script_mem_bytes / 1024 / 1024, 2)
        except:
            pass

        # 4. Disk (Root partition)
        # Only add to stats if we successfully read the disk
        try:
            total, used, free = shutil.disk_usage("/")
            stats["sys_disk"] = round((used / total) * 100, 1)
        except:
            # Do not add "sys_disk" key if this fails
            pass

        # 5. Temperature (Linux specific)
        # Only add to stats if we find a valid sensor
        try:
            temps = psutil.sensors_temperatures()
            found_temp = None

            if temps:
                if "cpu_thermal" in temps:
                    found_temp = temps["cpu_thermal"][0].current
                elif "coretemp" in temps:
                    found_temp = temps["coretemp"][0].current
                else:
                    # Fallback: grab the first available sensor
                    for name, entries in temps.items():
                        found_temp = entries[0].current
                        break

            if found_temp is not None:
                stats["sys_temp"] = found_temp
        except:
            pass

        # 6. Uptime
        stats["sys_uptime"] = int(time.time() - self.boot_time)

        # 7. Static/Network Info (Send every time so it's always available)
        stats["sys_os_version"] = self.os_info
        stats["sys_model"] = self.model_info

        try:
            # Trick to find the IP used for outgoing traffic (doesn't actually connect)
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            stats["sys_ip"] = s.getsockname()[0]
            s.close()
        except:
            stats["sys_ip"] = "127.0.0.1"

        return stats
