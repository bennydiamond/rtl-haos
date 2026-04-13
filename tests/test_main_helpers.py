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

    def publish_known_devices_select(self):
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
    
    def __init__(self, *args, **kwargs):
        pass

    def start_thread(self, *args, **kwargs):
        pass

    def stop_thread(self):
        pass

    def clear_all_devices(self):
        pass

    def queue_clear_all_devices(self):
        pass

    def queue_add_or_update_device(self, compound_id, device_name, new_topics):
        pass

    def get_known_devices(self):
        return set()

    def remove_device(self, compound_id):
        pass

    def queue_remove_device(self, compound_id):
        pass
