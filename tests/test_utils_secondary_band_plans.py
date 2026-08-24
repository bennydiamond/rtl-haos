from utils import choose_secondary_band_defaults, validate_radio_config


def test_choose_secondary_band_defaults_eu_and_us_plans():
    assert choose_secondary_band_defaults(plan="eu", country_code=None) == ("868M", 0)
    assert choose_secondary_band_defaults(plan="us", country_code=None) == ("915M", 0)


def test_choose_secondary_band_defaults_custom_no_override_falls_back_to_auto():
    # plan=custom but no override -> becomes auto with unknown country -> hop both
    assert choose_secondary_band_defaults(
        plan="custom", country_code=None, secondary_override=""
    ) == ("868M,915M", 15)


def test_validate_radio_config_numeric_hz_no_warning_branch():
    # Pure numeric with >=1,000,000 should NOT trigger "missing suffix" warning branch.
    warns = validate_radio_config({"freq": "433920000", "rate": "1024k", "id": "1"})
    assert not any("missing a suffix" in w.lower() for w in warns)
