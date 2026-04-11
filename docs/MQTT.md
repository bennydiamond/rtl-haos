# MQTT topics

RTL-HAOS publishes **Home Assistant MQTT Discovery** configs (retained) plus state topics (also retained).

## Availability

- `home/status/rtl_bridge<ID_SUFFIX>/availability`
  - payloads: `online` / `offline`

`ID_SUFFIX` is empty by default. If `FORCE_NEW_IDS=true`, it becomes `_v2` (see `config.py`).

## State topics

Sensor/binary-sensor state is published to:

- `home/rtl_devices/<clean_id>/<field>`

Where:

- `<clean_id>` is the device id from `rtl_433` normalized for MQTT/HA (see `utils.clean_mac`).
- `<field>` is the flattened field name (e.g., `temperature`, `humidity`, `rssi`, `snr`, `radio_status_101`).

State messages are published with `retain=True` so Home Assistant can restore values on restart.

## Home Assistant MQTT Discovery

Discovery configs are published (retained) to:

- `homeassistant/<domain>/<unique_id>/config`

Where:

- `<domain>` is usually `sensor`, but some fields publish as other domains:
  - `battery_ok` is published as a `binary_sensor` with device_class `battery` (ON = low).
- `<unique_id>` is built from `<clean_id>_<field><ID_SUFFIX>`.

Discovery payloads include:

- `state_topic` (points at `home/rtl_devices/<clean_id>/<field>`)
- `availability_topic` (points at the bridge availability topic)
- `expire_after` (defaults to `RTL_EXPIRE_AFTER`)

## Maintenance command topics

RTL-HAOS publishes several HA entities (buttons, switches, and selects) under the **Bridge** device for maintenance and configuration. When interacted with, Home Assistant sends commands to:

- `home/status/rtl_bridge<ID_SUFFIX>/nuke/set` (Delete Entities; press 5x)
- `home/status/rtl_bridge<ID_SUFFIX>/restart/set` (Restart Radios)
- `home/status/rtl_bridge<ID_SUFFIX>/remove_device/set` (Remove Selected Device button)
- `home/status/rtl_bridge<ID_SUFFIX>/known_devices/set` (Select Device to remove dropdown selection)
- `home/status/rtl_bridge<ID_SUFFIX>/discovery_new_devices/set` (Toggle New Device Discovery)

## Entity cleanup ("Delete Entities")

RTL-HAOS provides two mechanisms for cleaning up stale entities:

1. **Single Device Deletion:** By using the "Select Device to remove" dropdown and pressing "Remove Selected Device", RTL-HAOS will specifically target and overwrite the retained MQTT config and state topics for that single device. It also permanently deletes the device from the `known_devices.json` persistence file.

2. **Nuke All ("Delete Entities"):** The global cleanup routine subscribes to `homeassistant/+/+/config` and deletes retained discovery configs where the discovery payload has `device.manufacturer` containing `rtl-haos`. 
   
   *Note: If you run multiple RTL-HAOS bridges on the same broker, this global cleanup scan can accidentally remove discovery entries for all of them.*
