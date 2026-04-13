import pytest
import json
from mqtt_handler import HomeNodeMQTT

def test_discovery_payload_structure(mocker):
    """
    Verifies the Discovery Payload matches the exact 'Golden' structure 
    required by Home Assistant.
    """
    # 1. Setup
    mocker.patch("mqtt_handler.mqtt.Client")
    handler = HomeNodeMQTT(version="1.0.0")
    handler.start()
    
    # 2. Spy on the publish method
    mock_publish = handler.client.publish
    
    # 3. Trigger a discovery
    # Sending "temperature_C" which maps to device_class: temperature
    # send_sensor is async by default; use sync variant for deterministic payload assertions.
    handler.send_sensor_sync("device_123", "temperature_C", 25.5, "WeatherStn", "ModelX")
    
    # 4. Find the config payload
    config_payload = None
    for call in mock_publish.call_args_list:
        topic, payload = call.args[0], call.args[1]
        if "/config" in topic:
            config_payload = json.loads(payload)
            break
            
    assert config_payload is not None, "No config payload sent!"

    # 5. THE GOLDEN CHECKS
    # These keys are MANDATORY for Home Assistant Discovery
    assert "device" in config_payload
    assert "identifiers" in config_payload["device"]
    assert "name" in config_payload
    assert "state_topic" in config_payload
    assert "unique_id" in config_payload
    assert "availability_topic" in config_payload
    
    # Verify specific mappings for Temperature
    assert config_payload["device_class"] == "temperature"
    assert config_payload["state_class"] == "measurement"
    assert config_payload["unit_of_measurement"] == "°C"
    
    # Verify Device Registry connection
    dev_info = config_payload["device"]
    assert dev_info["manufacturer"] == "rtl-haos"
    assert "rtl433_ModelX_device123" in dev_info["identifiers"][0]