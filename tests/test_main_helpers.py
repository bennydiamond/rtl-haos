"""Shared test helpers for main.py tests."""


class DummyMQTT:
    """Standard mock MQTT handler for main.py tests."""
    
    def __init__(self, version=None, *args, **kwargs):
        self.version = version
        self.allow_new_device_discovery = True
        self.device_count_channel = type(
            "_Ch",
            (),
            {
                "loop": lambda self, *a, **k: None,
                "start_thread": lambda self, *a, **k: None,
            },
        )()

    def start(self):
        pass

    def stop(self):
        pass

    def _get_discovery_enabled(self):
        return self.allow_new_device_discovery

    def cleanup_device_discovered_topics(self, clean_id):
        pass


class DummyProcessor:
    """Standard mock data processor for main.py tests."""
    
    def __init__(self, mqtt, *args, **kwargs):
        self.mqtt = mqtt

    def start_throttle_loop(self):
        pass


class DummyThread:
    """Standard mock Thread for main.py tests."""
    
    def __init__(self, target=None, args=(), daemon=None):
        self.target = target
        self.args = args
        self.daemon = daemon

    def start(self):
        pass


class DummyKnownStore:
    """Standard mock known device store for main.py tests."""
    
    def __init__(self, path=None):
        self.path = path

    def load_ids(self):
        return set()

    def save_ids(self, ids):
        pass


class DummyKnownDeviceManager:
    """Standard mock known device manager for main.py tests."""
    
    def __init__(self, known_device_store=None, get_discovery_enabled_callback=None, mqtt_cleanup_callback=None):
        pass

    def clear_all_devices(self):
        pass
