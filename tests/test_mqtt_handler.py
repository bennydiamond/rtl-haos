import pytest
import json
from mqtt_handler import HomeNodeMQTT

def test_subscriptions_on_connect(mocker):
    """
    Verifies that we actually subscribe to the Command topics when we connect.
    """
    # 1. Setup
    mocker.patch("mqtt_handler.mqtt.Client")
    handler = HomeNodeMQTT()
    
    # Mock the subscribe method
    mock_subscribe = handler.client.subscribe
    
    # 2. Simulate the on_connect callback
    # Arguments: client, userdata, flags, rc
    handler._on_connect(handler.client, None, None, 0)
    
    # 3. Verify Subscriptions
    # We expect subscriptions to "nuke/set" and "restart/set"
    topics_subscribed = [call.args[0] for call in mock_subscribe.call_args_list]
    
    assert any("nuke/set" in t for t in topics_subscribed), "Failed to subscribe to Nuke command"
    assert any("restart/set" in t for t in topics_subscribed), "Failed to subscribe to Restart command"
    
def test_unknown_field_fallback(mocker):
    """
    Ensures that if a device sends a new/unknown field, we use default metadata 
    instead of crashing or dropping it.
    """
    # 1. Setup
    mocker.patch("mqtt_handler.mqtt.Client")
    handler = HomeNodeMQTT()
    handler.start()
    mock_publish = handler.client.publish
    
    # 2. Send a completely made-up field
    handler.send_sensor("device_x", "alien_radiation", 999, "UFO", "Saucer")
    handler.stop()
    
    # 3. Find the discovery payload
    payload = None
    for call in mock_publish.call_args_list:
        if "config" in call.args[0] and "alien_radiation" in call.args[0]:
            payload = json.loads(call.args[1])
            break
            
    # 4. Verify Fallbacks
    assert payload is not None
    assert payload["name"] == "Alien Radiation" # Auto-capitalized
    assert payload["icon"] == "mdi:eye"         # Default Icon
    assert "unit_of_measurement" not in payload # No default unit

def test_mqtt_discovery_payload(mocker, mock_config):
    """Verifies the JSON payload sent to Home Assistant config topic."""
    # Mock the internal client
    mock_paho = mocker.patch("mqtt_handler.mqtt.Client")
    mock_client_instance = mock_paho.return_value
    
    handler = HomeNodeMQTT()
    handler.start()
    
    # Simulate sending a sensor
    handler.send_sensor("device123", "temperature", 75.0, "My Weather", "ModelZ")
    handler.stop()
    
    # Verify Publish Calls
    publish_calls = mock_client_instance.publish.call_args_list
    
    found_discovery = False
    found_state = False
    
    for args, _ in publish_calls:
        topic = args[0]
        payload = args[1]
        
        if "config" in topic and "rtl433_ModelZ_device123_temperature" in topic:
            found_discovery = True
            data = json.loads(payload)
            assert data["device"]["name"] == "My Weather"
            assert data["device_class"] == "temperature"
            assert data["unit_of_measurement"] == "°F"
            
        if topic == "home/rtl_devices/rtl433_ModelZ_device123/temperature":
            found_state = True
            assert payload == "75.0"

    assert found_discovery, "Discovery topic not published"
    assert found_state, "State topic not published"

def test_nuke_logic(mocker):
    """Verifies the 5-press safety mechanism for nuking."""
    handler = HomeNodeMQTT()
    
    # Mock the internal Nuke method so we don't actually delete things
    mock_nuke_all = mocker.patch.object(handler, "nuke_all")
    
    # Press 1 through 4 (Should NOT trigger)
    for i in range(4):
        handler._handle_nuke_press()
        assert handler.nuke_counter == i + 1
        mock_nuke_all.assert_not_called()
        
    # Press 5 (Should TRIGGER)
    handler._handle_nuke_press()
    mock_nuke_all.assert_called_once()
    assert handler.nuke_counter == 0 # Should reset