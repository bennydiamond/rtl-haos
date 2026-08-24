import pytest
from field_meta import FIELD_META


def test_critical_fields_exist():
    """
    Ensures that critical sensor definitions are never deleted from FIELD_META.
    """
    # List of keys that MUST exist for the system to be useful
    critical_keys = ["temperature", "humidity", "rssi", "snr", "noise", "sys_device_count", "freq"]

    for key in critical_keys:
        assert key in FIELD_META, f"CRITICAL: Key '{key}' was deleted from FIELD_META!"


def test_field_structure():
    """
    Ensures no one broke the tuple structure (Unit, Class, Icon, Name).
    """
    for key, val in FIELD_META.items():
        assert isinstance(val, tuple), f"Key {key} is not a tuple!"
        assert len(val) == 4, f"Key {key} does not have 4 elements (Unit, Class, Icon, Name)"
