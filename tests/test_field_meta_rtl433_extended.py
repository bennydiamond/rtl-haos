import pytest

import field_meta


# These are high-impact rtl_433 fields we added support for in field_meta.py.
# The goal is to lock down mapping stability (units, device_class, icon, friendly name)
# so future refactors don't silently regress HA discovery metadata.
EXPECTED = {
    # Battery
    "battery_pct": ('%', 'battery', 'mdi:battery', 'Battery'),
    "battery_V": ('V', 'voltage', 'mdi:battery', 'Battery Voltage'),
    "battery_mV": ('mV', 'voltage', 'mdi:battery', 'Battery Voltage'),
    "battery_low": (None, 'none', 'mdi:battery-alert', 'Battery Low (Raw)'),
    "battery_raw": ('cnt', 'none', 'mdi:battery', 'Battery Raw'),

    # Pressure
    "pressure_hPa": ('hPa', 'pressure', 'mdi:gauge', 'Pressure'),
    "pressure_kPa": ('kPa', 'pressure', 'mdi:gauge', 'Pressure'),
    "pressure_psi": ('psi', 'pressure', 'mdi:gauge', 'Pressure'),

    # Wind
    "wind_avg_m_s": ('m/s', 'wind_speed', 'mdi:weather-windy', 'Wind Speed'),
    "wind_max_m_s": ('m/s', 'wind_speed', 'mdi:weather-windy-variant', 'Wind Gust'),
    "wind_max_km_h": ('km/h', 'wind_speed', 'mdi:weather-windy-variant', 'Wind Gust'),
    "wind_max_mi_h": ('mph', 'wind_speed', 'mdi:weather-windy-variant', 'Wind Gust'),

    # Light / UV
    "light_lux": ('lx', 'illuminance', 'mdi:brightness-5', 'Light Level'),
    "uvi": ('UV Index', 'none', 'mdi:sunglasses', 'UV Index'),

    # Multi-probe temp/humidity and thermostat setpoint
    "temperature_1_C": ('°C', 'temperature', 'mdi:thermometer', 'Temperature 1 (C)'),
    "temperature_2_C": ('°C', 'temperature', 'mdi:thermometer', 'Temperature 2 (C)'),
    "temperature_3_C": ('°C', 'temperature', 'mdi:thermometer', 'Temperature 3 (C)'),
    "temperature_4_C": ('°C', 'temperature', 'mdi:thermometer', 'Temperature 4 (C)'),
    "temperature_1_F": ('°F', 'temperature', 'mdi:thermometer', 'Temperature 1'),
    "temperature_2_F": ('°F', 'temperature', 'mdi:thermometer', 'Temperature 2'),
    "humidity_1": ('%', 'humidity', 'mdi:water-percent', 'Humidity 1'),
    "humidity_2": ('%', 'humidity', 'mdi:water-percent', 'Humidity 2'),
    "setpoint_C": ('°C', 'temperature', 'mdi:thermostat', 'Setpoint (C)'),
    "setpoint_F": ('°F', 'temperature', 'mdi:thermostat', 'Setpoint'),

    # Air quality
    "co2_ppm": ('ppm', 'carbon_dioxide', 'mdi:molecule-co2', 'CO₂ Level'),
    "pm2_5_ug_m3": ('µg/m³', 'pm25', 'mdi:blur', 'PM2.5'),
    "pm10_ug_m3": ('µg/m³', 'pm10', 'mdi:blur', 'PM10'),
    "pm10_0_ug_m3": ('µg/m³', 'pm10', 'mdi:blur', 'PM10'),
    "estimated_pm10_0_ug_m3": ('µg/m³', 'pm10', 'mdi:blur', 'PM10 (Estimated)'),
    "pm1_ug_m3": ('µg/m³', 'none', 'mdi:blur', 'PM1.0'),
    "pm4_ug_m3": ('µg/m³', 'none', 'mdi:blur', 'PM4.0'),

    # Power / energy
    "power_W": ('W', 'power', 'mdi:flash', 'Power'),
    "power0_W": ('W', 'power', 'mdi:flash', 'Power 0'),
    "power1_W": ('W', 'power', 'mdi:flash', 'Power 1'),
    "power2_W": ('W', 'power', 'mdi:flash', 'Power 2'),
    "power3_W": ('W', 'power', 'mdi:flash', 'Power 3'),
    "energy_kWh": ('kWh', 'energy', 'mdi:counter', 'Energy'),
    "total_kWh": ('kWh', 'energy', 'mdi:counter', 'Energy Total'),
    "voltage_V": ('V', 'voltage', 'mdi:sine-wave', 'Voltage'),
    "current_A": ('A', 'current', 'mdi:current-ac', 'Current'),

    # Radio diagnostics
    "last_seen": (None, 'timestamp', 'mdi:clock-outline', 'Last Seen'),
}


@pytest.mark.parametrize("field, expected", sorted(EXPECTED.items()))
def test_field_meta_includes_expected_rtl433_fields(field, expected):
    assert field_meta.FIELD_META.get(field) == expected


def test_signal_strength_alias_fields_match_base_fields():
    # These are common rtl_433 variants; keep them consistent.
    assert field_meta.FIELD_META["rssi_dB"] == field_meta.FIELD_META["rssi"]
    assert field_meta.FIELD_META["snr_dB"] == field_meta.FIELD_META["snr"]
    assert field_meta.FIELD_META["noise_dB"] == field_meta.FIELD_META["noise"]


@pytest.mark.parametrize("field", sorted(EXPECTED.keys()))
def test_get_field_meta_returns_same_as_field_meta_dict(field):
    assert field_meta.get_field_meta(field) == field_meta.FIELD_META[field]


def test_field_meta_entries_are_well_formed():
    # Sanity: every entry is a 4-tuple and has a non-empty friendly name,
    # and icons are mdi:* when present.
    for key, meta in field_meta.FIELD_META.items():
        assert isinstance(meta, tuple) and len(meta) == 4, (key, meta)
        unit, device_class, icon, friendly = meta
        assert device_class is not None and str(device_class).strip() != "", (key, meta)
        assert friendly is not None and str(friendly).strip() != "", (key, meta)
        if icon is not None:
            assert str(icon).startswith("mdi:"), (key, meta)
