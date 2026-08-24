from mqtt_handler import (
    infer_commodity_from_ert_type,
    infer_commodity_from_meter_type,
    infer_commodity_from_type_field,
)


def test_infer_commodity_from_ert_type_invalid_returns_none():
    assert infer_commodity_from_ert_type(None) is None
    assert infer_commodity_from_ert_type("not-a-number") is None


def test_infer_commodity_from_meter_type_non_string_returns_none():
    assert infer_commodity_from_meter_type(123) is None


def test_infer_commodity_from_type_field_covers_common_strings():
    assert infer_commodity_from_type_field("Gas") == "gas"
    assert infer_commodity_from_type_field("WATER") == "water"
    assert infer_commodity_from_type_field("electric") == "electric"
