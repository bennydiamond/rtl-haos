from field_meta import get_field_meta


def test_get_field_meta_model_present_but_field_missing_falls_back_to_base_meta():
    base = {"unknown_field": ("u", "dc", "mdi:test", "Unknown")}
    # Model matches known prefix, but the field is not present in that model mapping.
    # Should fall through to base_meta.
    assert get_field_meta("unknown_field", device_model="Neptune-R900", base_meta=base) == (
        "u",
        "dc",
        "mdi:test",
        "Unknown",
    )
