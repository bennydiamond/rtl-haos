import runpy


def test_system_monitor_main_block_starts_and_stops(monkeypatch):
    """Exercise the __main__ block without starting background loops."""

    import mqtt_handler
    import system_monitor
    import threading
    import time
    import utils

    # --- Patch dependencies the __main__ block uses ---
    class DummyMQTT:
        last_instance = None

        def __init__(self, *a, **k):
            DummyMQTT.last_instance = self
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

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

    created_threads = []

    class DummyThread:
        def __init__(self, *a, **k):
            created_threads.append((a, k))

        def start(self):
            return None

    monkeypatch.setattr(utils, "get_system_mac", lambda: "AA:BB:CC:DD:EE:FF")
    monkeypatch.setattr(mqtt_handler, "HomeNodeMQTT", DummyMQTT)
    monkeypatch.setattr(threading, "Thread", DummyThread)

    def sleep(sec):
        # Break the infinite loop immediately.
        if sec == 1:
            raise KeyboardInterrupt
        return None

    monkeypatch.setattr(time, "sleep", sleep)

    # Execute the file as if it were run as a script.
    runpy.run_path(system_monitor.__file__, run_name="__main__")

    inst = DummyMQTT.last_instance
    assert inst is not None
    assert inst.started is True
    assert inst.stopped is True
    assert created_threads, "expected system_stats_loop thread setup"