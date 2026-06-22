# field_meta.py
"""
FILE: field_meta.py
"""
# Format: (Unit, Device Class, Icon, Friendly Name)

FIELD_META = {
    # --- System Diagnostics (NEW: Configuration) ---
    # "sys_cfg_blacklist":    ("", "none", "mdi:playlist-remove", "Blacklist"),
    # "sys_cfg_whitelist":    ("", "none", "mdi:playlist-check", "Whitelist"),
    # "sys_cfg_sensors":      ("", "none", "mdi:eye-settings", "Main Sensors"),

    # --- SDR Health Monitoring ---
    "sdr_health_alert":     (None, "problem", "mdi:alert-octagon", "SDR Health Alert"),
    "sdr_health_reason":    (None, "none", "mdi:information", "Health Alert Reason"),

    # --- System Diagnostics (Existing) ---
    "sys_device_count":     ("dev", "none", "mdi:counter", "Active Devices"),
    # "sys_device_list":      ("", "none", "mdi:format-list-bulleted", "Device List"),

    "sys_ip":               ("", "none", "mdi:ip-network", "IP Address"),
    "sys_os_version":       ("", "none", "mdi:linux", "Linux Kernel"),
    "sys_rtl_433_version": ("", "none", "mdi:radio", "rtl_433 Version"),
    "sys_model":            ("", "none", "mdi:chip", "Device Model"),
    "sys_script_mem":       ("MB", "data_size", "mdi:memory", "Script RAM Usage"),
    "sys_cpu":              ("%", "none", "mdi:cpu-64-bit", "CPU Load"),
    "sys_mem":              ("%", "none", "mdi:memory", "RAM Usage"),
    "sys_disk":             ("%", "none", "mdi:harddisk", "Disk Usage"),
    "sys_temp":             ("°C", "temperature", "mdi:thermometer-lines", "CPU Temp"),
    "sys_uptime":           ("s", "duration", "mdi:clock-start", "System Uptime"),
    "sys_bridge_uptime":    ("s", "duration", "mdi:timer-outline", "RTL-HAOS Uptime"),
    "model":                ("", "none", "mdi:tag", "Model"),

    # --- Magnetometer ---
    "mag_uT":               ("uT", "none", "mdi:magnet", "Mag Field Strength"),
    "geomag_index":         ("idx", "none", "mdi:waveform", "Mag Disturbance"),
    "status":               ("", "enum", "mdi:list-status", "Device Status"),

    # --- Temperature ---
    "temperature":          ("°F", "temperature", "mdi:thermometer", "Temperature"),
    "temperature_C":        ("°C", "temperature", "mdi:thermometer", "Temperature (C)"),
    "temperature_F":        ("°F", "temperature", "mdi:thermometer", "Temperature"),
    "setpoint_C":          ("°C", "temperature", "mdi:thermostat", "Setpoint (C)"),
    "setpoint_F":          ("°F", "temperature", "mdi:thermostat", "Setpoint"),
    "temperature_1_C":     ("°C", "temperature", "mdi:thermometer", "Temperature 1 (C)"),
    "temperature_2_C":     ("°C", "temperature", "mdi:thermometer", "Temperature 2 (C)"),
    "temperature_3_C":     ("°C", "temperature", "mdi:thermometer", "Temperature 3 (C)"),
    "temperature_4_C":     ("°C", "temperature", "mdi:thermometer", "Temperature 4 (C)"),
    "temperature_1_F":     ("°F", "temperature", "mdi:thermometer", "Temperature 1"),
    "temperature_2_F":     ("°F", "temperature", "mdi:thermometer", "Temperature 2"),
    "temperature_2":       ("°F", "temperature", "mdi:thermometer", "Temperature 2"),
    "dew_point":            ("°F", "temperature", "mdi:weather-fog", "Dew Point"),

    # --- Humidity ---
    "humidity":             ("%", "humidity", "mdi:water-percent", "Humidity"),
    "humidity_1":          ("%", "humidity", "mdi:water-percent", "Humidity 1"),
    "humidity_2":          ("%", "humidity", "mdi:water-percent", "Humidity 2"),

    # --- Air Quality ---
    "co2":                  ("ppm", "carbon_dioxide", "mdi:molecule-co2", "CO2 Level"),
    "co2_ppm":             ("ppm", "carbon_dioxide", "mdi:molecule-co2", "CO₂ Level"),
    "pm2_5_ug_m3":          ("µg/m³", "pm25", "mdi:blur", "PM2.5"),
    "pm10_ug_m3":           ("µg/m³", "pm10", "mdi:blur", "PM10"),
    "pm10_0_ug_m3":         ("µg/m³", "pm10", "mdi:blur", "PM10"),
    "estimated_pm10_0_ug_m3": ("µg/m³", "pm10", "mdi:blur", "PM10 (Estimated)"),
    "pm1_ug_m3":            ("µg/m³", "none", "mdi:blur", "PM1.0"),
    "pm4_ug_m3":            ("µg/m³", "none", "mdi:blur", "PM4.0"),

    # --- Pressure ---
    "pressure_hpa":         ("hPa", "pressure", "mdi:gauge", "Pressure"),
    "pressure_inhg":        ("inHg", "pressure", "mdi:gauge", "Pressure"),
    "pressure_PSI":         ("psi", "pressure", "mdi:gauge", "Pressure"),
    "pressure_hPa":        ("hPa", "pressure", "mdi:gauge", "Pressure"),
    "pressure_kPa":        ("kPa", "pressure", "mdi:gauge", "Pressure"),
    "pressure_psi":        ("psi", "pressure", "mdi:gauge", "Pressure"),

    # --- Wind ---
    "wind_avg_km_h":        ("km/h", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_avg_mi_h":        ("mph", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_avg_m_s":        ("m/s", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_speed":           ("km/h", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_speed_km_h":      ("km/h", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_speed_m_s":       ("m/s", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_speed_mi_h":      ("mph", "wind_speed", "mdi:weather-windy", "Wind Speed"),
    "wind_gust_km_h":       ("km/h", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_gust_mi_h":       ("mph", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_gust_m_s":        ("m/s", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "gust_speed_km_h":      ("km/h", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "gust_speed_m_s":       ("m/s", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_max_m_s":        ("m/s", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_max_km_h":       ("km/h", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_max_mi_h":       ("mph", "wind_speed", "mdi:weather-windy-variant", "Wind Gust"),
    "wind_dir_deg":         ("°", "wind_direction", "mdi:compass", "Wind Direction"),
    "wind_dir":             ("°", "wind_direction", "mdi:compass", "Wind Direction"),
    "wind_dev_deg":         ("°", "none", "mdi:compass-rose", "Wind Deviation"),

    # --- Rain ---
    "rain_mm":              ("mm", "precipitation", "mdi:weather-rainy", "Rain Total"),
    "rain_in":              ("in", "precipitation", "mdi:weather-rainy", "Rain Total"),
    "rain_rate_mm_h":       ("mm/h", "precipitation_intensity", "mdi:weather-pouring", "Rain Rate"),
    "rain_rate_in_h":       ("in/h", "precipitation_intensity", "mdi:weather-pouring", "Rain Rate"),
    "rain_start":           (None, "none", "mdi:weather-rainy", "Rain Detected"),
    "rain2_mm":             ("mm", "precipitation", "mdi:weather-rainy", "Rain Total 2"),
    "rain_raw":             ("count", "none", "mdi:weather-rainy", "Rain Raw Count"),
    "rain":                 ("count", "none", "mdi:weather-rainy", "Rain Count"),
    "rain1":                ("count", "none", "mdi:weather-rainy", "Rain Count 1"),
    "rain2":                ("count", "none", "mdi:weather-rainy", "Rain Count 2"),

    # --- Light ---
    "lux":                  ("lx", "illuminance", "mdi:brightness-5", "Light Level"),
    "light_lux":           ("lx", "illuminance", "mdi:brightness-5", "Light Level"),
    "uvi":                 ("UV Index", "none", "mdi:sunglasses", "UV Index"),
    "uv_index":            ("UV Index", "none", "mdi:sunglasses", "UV Index"),
    "full_lux":             ("cnt", "none", "mdi:brightness-7", "Raw Full Spectrum"),
    "ir_lux":               ("cnt", "none", "mdi:cctv", "Raw IR"),
    "uv":                   ("UV Index", "none", "mdi:sunglasses", "UV Index"),
    "wm":                   ("W/m²", "irradiance", "mdi:white-balance-sunny", "Solar Radiation"),
    "uv_sensor_id":         (None, "none", "mdi:identifier", "UV Sensor ID"),
    "uv_status":            (None, "none", "mdi:check-circle", "UV Sensor Status"),
    "exposure_mins":        ("min", "duration", "mdi:sun-clock", "UV Exposure Time"),

    # --- Lightning ---
    "strikes":              ("count", "none", "mdi:flash", "Lightning Strikes"),
    "strike_distance":      ("km", "distance", "mdi:flash-alert", "Storm Distance"),
    "storm_dist":           ("km", "distance", "mdi:flash-alert", "Storm Distance"),
    "storm_distance":       ("km", "distance", "mdi:flash-alert", "Storm Distance"),
    "storm_dist_km":        ("km", "distance", "mdi:flash-alert", "Storm Distance"),
    "strike_count":         (None, "none", "mdi:lightning-bolt", "Strike Count"),
    "active":               (None, "none", "mdi:flash", "Lightning Active"),

    # --- Soil Moisture ---
    "moisture":            ("%", "moisture", "mdi:water-percent", "Soil Moisture"),

    # --- Leak Detection (Acurite 1190/1192) ---
    "leak_detected":       (None, "moisture", "mdi:water-alert", "Leak Detected"),
    "water":               (None, "moisture", "mdi:water", "Water Detected"),
    

    # --- Radio Diagnostics ---
    "last_seen":            (None, "timestamp", "mdi:clock-outline", "Last Seen"),
    "freq":                 ("MHz", "frequency", "mdi:sine-wave", "Frequency"),
    "freq1":                ("MHz", "frequency", "mdi:sine-wave", "Frequency"),
    "freq2":                ("MHz", "frequency", "mdi:sine-wave", "Frequency"),
    "mod":                  ("", "none", "mdi:waveform", "Modulation"),
    "modulation":           ("", "none", "mdi:waveform", "Modulation"),
    "rssi":                 ("dB", "signal_strength", "mdi:wifi", "Signal (RSSI)"),
    "snr":                  ("dB", "signal_strength", "mdi:signal-distance-variant", "Signal (SNR)"),
    "noise":                ("dB", "signal_strength", "mdi:volume-high", "Noise Floor"),
    "rssi_dB":             ("dB", "signal_strength", "mdi:wifi", "Signal (RSSI)"),
    "snr_dB":              ("dB", "signal_strength", "mdi:signal-distance-variant", "Signal (SNR)"),
    "noise_dB":            ("dB", "signal_strength", "mdi:volume-high", "Noise Floor"),
    "flags":               (None, "none", "mdi:flag", "Flags"),
    "button":              (None, "none", "mdi:button-pointer", "Button"),
    "code":                (None, "none", "mdi:remote", "Code"),
    "state":               (None, "none", "mdi:toggle-switch", "State"),
    "counter":             ("count", "none", "mdi:counter", "Counter"),
    "sequence":            ("count", "none", "mdi:counter", "Sequence"),
    "version":             (None, "none", "mdi:tag", "Version"),
    "type":                (None, "none", "mdi:tag-outline", "Type"),
    "subtype":             (None, "none", "mdi:tag-outline", "Subtype"),
    "id":                   ("", "none", "mdi:identifier", "Device ID"),
    "channel":              ("", "none", "mdi:radio-tower", "Channel"),
    "mic":                  ("", "none", "mdi:check-network", "Integrity Check"),
    "radio_status":         ("", "none", "mdi:radio-tower", "Radio Status"),
    "rfi":                  (None, "none", "mdi:radio-tower", "RFI"),
    "radio_clock":          (None, "timestamp", "mdi:radio-tower", "Radio Clock"),
    "signal":               (None, "none", "mdi:signal", "Signal Type"),
    "firmware":             (None, "none", "mdi:chip", "Firmware"),
    "sensitivity":          (None, "none", "mdi:tune", "Sensitivity"),
    "raw_value":            (None, "none", "mdi:numeric", "Raw Value"),
    "ad_raw":               (None, "none", "mdi:numeric", "ADC Raw"),
    "boost":                (None, "none", "mdi:signal-cellular-3", "Boost Mode"),
    "msg_type":             (None, "none", "mdi:message-text", "Message Type"),
    "data":                 (None, "none", "mdi:code-braces", "Extra Data"),
    "ptemp_raw":            (None, "none", "mdi:thermometer", "Raw Temperature"),
    "phumidity":            (None, "none", "mdi:water-percent", "Raw Humidity"),

    # --- Raw Data ---
    # Raw hex message from rtl_433, useful for debugging or protocol analysis.
    "raw_msg":              (None, "none", "mdi:code-tags", "Raw Message"),

    # --- Timestamp ---
    # rtl_433 outputs a "time" field when run with -M time or -M utc.
    # This is useful to see when a device last transmitted, even if values didn't change.
    "time":                 (None, "timestamp", "mdi:clock-in", "Last Seen"),
    "sequence_num":         (None, "none", "mdi:counter", "Sequence"),
    "message_type":         (None, "none", "mdi:message-text", "Message Type"),
    "exception":            (None, "none", "mdi:alert-circle", "Exception"),
    "seq":                  (None, "none", "mdi:counter", "Sequence"),
    "startup":              (None, "none", "mdi:power", "Startup"),
    "test":                 (None, "none", "mdi:test-tube", "Test Mode"),

    # --- Depth / Level ---
    "depth_cm":             ("cm", "distance", "mdi:arrow-collapse-down", "Depth"),
    "depth_mm":             ("mm", "distance", "mdi:arrow-collapse-down", "Depth"),
    "depth_in":             ("in", "distance", "mdi:arrow-collapse-down", "Depth"),

    # --- Utility Meters ---
    "Consumption":          ("ft³", "gas", "mdi:fire", "Gas Usage"),
    "consumption":          ("ft³", "gas", "mdi:fire", "Gas Usage"),
    "consumption_data":     ("ft³", "gas", "mdi:fire", "Gas Usage"),
    "meter_reading":        ("ft³", "water", "mdi:water-pump", "Water Reading"),
    # Common rtl_433 water meter fields
    # - Badger ORION emits volume_gal
    # - Many wireless meter protocols expose volume in common units
    "volume_gal":           ("gal", "water", "mdi:water-pump", "Water Usage"),
    "volume_ft3":           ("ft³", "water", "mdi:water-pump", "Water Usage"),
    "volume_m3":            ("m³", "water", "mdi:water-pump", "Water Usage"),
    "total_m3":             ("m³", "water", "mdi:water-pump", "Water Total"),
    "total_l":              ("L",  "water", "mdi:water-pump", "Water Total"),
    "consumption_at_set_date_m3": ("m³", "water", "mdi:water-pump", "Water @ Set Date"),

    # --- Power / Energy ---
    "power_W":             ("W", "power", "mdi:flash", "Power"),
    "power0_W":            ("W", "power", "mdi:flash", "Power 0"),
    "power1_W":            ("W", "power", "mdi:flash", "Power 1"),
    "power2_W":            ("W", "power", "mdi:flash", "Power 2"),
    "power3_W":            ("W", "power", "mdi:flash", "Power 3"),
    "energy_kWh":          ("kWh", "energy", "mdi:counter", "Energy"),
    "total_kWh":           ("kWh", "energy", "mdi:counter", "Energy Total"),
    "voltage_V":           ("V", "voltage", "mdi:sine-wave", "Voltage"),
    "current_A":           ("A", "current", "mdi:current-ac", "Current"),


    # --- Security / Binary Sensors ---
    # These fields are published as binary_sensors with appropriate device classes.
    # The actual binary_sensor logic is in mqtt_handler.py BINARY_SENSOR_FIELDS.
    "tamper":               (None, "tamper", "mdi:alert-circle", "Tamper"),
    "alarm":                (None, "safety", "mdi:alarm-light", "Alarm"),
    "contact_open":         (None, "door", "mdi:door", "Door"),
    "reed_open":            (None, "door", "mdi:door", "Door"),
    "detect_wet":           (None, "moisture", "mdi:water-alert", "Water Detected"),
    "ext_power":            (None, "plug", "mdi:power-plug", "External Power"),

    # --- Battery ---
    # Many decoders emit battery_ok where 1/True means battery is OK and 0/False
    # means battery is LOW. We publish this as a binary sensor (device_class: battery)
    # and invert it in mqtt_handler so ON means LOW battery.
    "battery_ok":           (None, "battery", "mdi:battery", "Battery Low"),
    "battery_pct":         ("%", "battery", "mdi:battery", "Battery"),
    "battery_V":           ("V", "voltage", "mdi:battery", "Battery Voltage"),
    "battery_mV":          ("mV", "voltage", "mdi:battery", "Battery Voltage"),
    "battery_low":         (None, "none", "mdi:battery-alert", "Battery Low (Raw)"),
    "battery_raw":         ("cnt", "none", "mdi:battery", "Battery Raw"),
    "battery_level":       (None, "none", "mdi:battery", "Battery Level"),
    "supercap_V":          ("V", "voltage", "mdi:solar-power", "Supercapacitor"),
    "newbattery":          (None, "none", "mdi:battery-plus", "New Battery"),

}

# Per-model overrides for MQTT discovery metadata.
# This keeps FIELD_META as conservative defaults while allowing correct units/names for specific devices.
#
# Keys are lowercase model prefixes (e.g. "neptune-r900") matched with startswith() after stripping.
MODEL_FIELD_META = {
    "neptune-r900": {
        # Neptune-R900 readings are normalized to gallons upstream (often tenths-of-gallon).
        "meter_reading": ("gal", "water", "mdi:water-pump", "Water Usage"),
    },
}

def get_field_meta(field: str, device_model: str | None = None, base_meta: dict | None = None):
    """Return (unit, device_class, icon, friendly_name) for a field, optionally model-aware.

    This is designed to be *backwards compatible* with existing code/tests that monkeypatch
    the `FIELD_META` dict from other modules (e.g., mqtt_handler.FIELD_META). Pass the dict
    you want to consult via `base_meta`.
    """
    if device_model:
        model_norm = str(device_model).strip().lower()
        for prefix, mapping in MODEL_FIELD_META.items():
            if model_norm.startswith(prefix):
                meta = mapping.get(field)
                if meta is not None:
                    return meta

    meta_source = base_meta if base_meta is not None else FIELD_META
    return meta_source.get(field)
