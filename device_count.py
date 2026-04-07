"""
FILE: device_count.py
DESCRIPTION:
  Encapsulates the device-count publishing channel.

  DeviceCountChannel owns:
    - A depth-1 Queue used to pass the latest device count from the main
      threads (via push()) to the monitor thread (via loop()).
    - push(count): flush any stale value then insert the new one.
    - loop(device_id, model_name): thread target that blocks on the queue
      and publishes sys_device_count via the bound mqtt_handler.
"""
import queue
import threading
import time


class DeviceCountChannel:
    """Depth-1 channel for propagating device count to the monitor thread.

    The producer (main/MQTT thread) calls push() to deliver the latest count.
    The consumer thread runs loop() which blocks until a new value arrives or
    60 s elapses (heartbeat), then publishes sys_device_count via mqtt_handler.

    Design guarantees:
      - Queue depth never exceeds 1: push() flushes any stale entry first.
      - The monitor thread never reads tracked_devices directly, eliminating
        concurrent access to that shared set.
    """

    def __init__(self, mqtt_handler) -> None:
        self._mqtt = mqtt_handler
        self._queue: queue.Queue = queue.Queue(maxsize=1)

    def push(self, count: int) -> None:
        """Overwrite the queued count.  Thread-safe; may be called from any thread."""
        try:
            self._queue.get_nowait()
        except queue.Empty:
            pass
        self._queue.put_nowait(count)

    def loop(self, device_id: str, model_name: str) -> None:
        """Thread target: publish sys_device_count on change or every 60 s."""
        print("[STARTUP] Starting Device Count Loop...")

        last_count = 0

        while True:
            device_name = f"{model_name} ({device_id})"

            try:
                last_count = self._queue.get(timeout=60)
            except queue.Empty:
                pass  # 60 s heartbeat — re-publish last known count

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
        thread = thread_factory(target=self.loop, args=(device_id, model_name), daemon=True)
        thread.start()
        return thread
