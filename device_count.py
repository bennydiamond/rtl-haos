"""
FILE: device_count.py
DESCRIPTION:
  Encapsulates the device-count publishing channel.

  DeviceCountChannel owns:
    - A lock-protected latest-count slot shared by producers.
    - An Event used to wake the consumer thread immediately on updates.
    - push(count): overwrite the latest count and signal the consumer.
    - loop(device_id, model_name): thread target that publishes the latest
      sys_device_count immediately on change and every 60 s as a heartbeat.
"""
import threading


class DeviceCountChannel:
    """Latest-value channel for propagating device count to the monitor thread.

    The producer (main/MQTT thread) calls push() to deliver the latest count.
    The consumer thread runs loop() which blocks until an update arrives or
    60 s elapses (heartbeat), then publishes sys_device_count via mqtt_handler.

    Design guarantees:
      - Only the latest count is retained; older pending values are overwritten.
      - The monitor thread never reads tracked_devices directly, eliminating
        concurrent access to that shared set.
    """

    def __init__(self, mqtt_handler) -> None:
        self._mqtt = mqtt_handler
        self._count_lock = threading.Lock()
        self._count_event = threading.Event()
        self._count_last = 0
        self._count_pending = False

    def push(self, count: int) -> None:
        """Overwrite the latest count and wake the consumer thread."""
        with self._count_lock:
            self._count_last = int(count)
            self._count_pending = True
            self._count_event.set()

    def loop(self, device_id: str, model_name: str) -> None:
        """Thread target: publish sys_device_count on change or every 60 s."""
        print("[STARTUP] Starting Device Count Loop...")

        last_count = 0

        while True:
            device_name = f"{model_name} ({device_id})"

            signaled = self._count_event.wait(timeout=60)
            with self._count_lock:
                if signaled and self._count_pending:
                    last_count = self._count_last
                    self._count_pending = False
                    self._count_event.clear()

            try:
                self._mqtt.send_sensor(
                    device_id,
                    "sys_device_count",
                    last_count,
                    device_name,
                    model_name,
                    is_rtl=True,
                )
            except Exception as e:
                print(f"[ERROR] Device Count update failed: {e}")

    def start_thread(self, device_id: str, model_name: str, thread_factory=threading.Thread):
        """Create and start the device-count loop thread.

        The optional thread_factory makes this test-friendly (callers can pass a
        patched threading.Thread).
        """
        kwargs = {
            "target": self.loop,
            "args": (device_id, model_name),
            "daemon": True,
        }
        # Use a stable, descriptive name in production for easier thread debugging.
        if thread_factory is threading.Thread:
            kwargs["name"] = "device_count_loop"

        thread = thread_factory(**kwargs)
        thread.start()
        return thread
