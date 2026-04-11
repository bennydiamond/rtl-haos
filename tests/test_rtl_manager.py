import pytest
import json
import time
from rtl_manager import rtl_loop, discover_rtl_devices, flatten

@pytest.fixture
def mock_subprocess(mocker):
    return mocker.patch("subprocess.Popen")
from rtl_manager import trigger_radio_restart, ACTIVE_PROCESSES

def test_trigger_radio_restart(mocker):
    """
    Verifies that calling restart actually terminates running processes.
    """
    # 1. Create a Fake Process
    mock_proc = mocker.Mock()
    # It returns None (meaning it is still running)
    mock_proc.poll.return_value = None 
    
    # 2. Add it to the global list
    ACTIVE_PROCESSES.append(mock_proc)
    
    # 3. Trigger Restart
    trigger_radio_restart()
    
    # 4. Verify terminate was called
    mock_proc.terminate.assert_called_once()
    
    # Cleanup (remove mock from global list)
    ACTIVE_PROCESSES.clear()
    
def test_device_discovery(mocker):
    """Mocks rtl_eeprom output to find devices."""
    mock_run = mocker.patch("subprocess.run")
    
    def side_effect_callback(cmd, **kwargs):
        mock_proc = mocker.Mock()
        mock_proc.returncode = 0
        mock_proc.stderr = ""
        
        # cmd looks like ['rtl_eeprom', '-d', '0']
        index = cmd[2] 
        
        if index == "0":
            mock_proc.stdout = """
Found 1 device(s):
  0:  Generic RTL2832U OEM
Current configuration:
__________________________________________
Vendor ID:      0x0bda
Product ID:     0x2838
Manufacturer:   Realtek
Product:        RTL2838UHIDIR
Serial number:  00000101
Serial number enabled:  yes
__________________________________________
            """
        else:
            mock_proc.stdout = "No supported devices found."
            mock_proc.returncode = 1
            
        return mock_proc

    mock_run.side_effect = side_effect_callback
    
    devices = discover_rtl_devices()
    
    found = [d for d in devices if d['id'] == '00000101']
    assert len(found) == 1
    assert found[0]['index'] == 0

def test_rtl_loop_parsing(mocker, mock_subprocess):
    """Feeds fake JSON lines to the loop and checks if data_processor gets them."""
    
    # 1. Setup Mock Process
    mock_proc = mocker.Mock()
    # Simulate stdout.readline() yielding 3 lines then returning empty string (EOF)
    fake_output = [
        '{"model": "FineOffset", "id": 123, "temperature_C": 20.5, "humidity": 50}\n',
        '{"model": "SimpliSafe", "id": 999, "state": "Open"}\n', # Should be blacklisted
        '{"model": "NewDev", "id": 456, "temperature_F": 70.0}\n',
        "" 
    ]
    mock_proc.stdout.readline.side_effect = fake_output
    # Poll returns None (running) 3 times, then 1 (dead)
    mock_proc.poll.side_effect = [None, None, None, 1] 
    
    mock_subprocess.return_value = mock_proc
    
    # 2. Setup Mocks for Handler/Processor
    mock_mqtt = mocker.Mock()
    mock_proc_logic = mocker.Mock()
    
    config_radio = {"name": "TestRadio", "id": "100", "freq": "433.92M"}

    # 3. BREAK THE INFINITE LOOP
    # rtl_loop calls time.sleep(5) before restarting. We mock this to raise an exception.
    mock_sleep = mocker.patch("rtl_manager.time.sleep")
    mock_sleep.side_effect = InterruptedError("Stop Test Loop")
    
    # 4. Run Loop
    # It will process lines, finish, try to sleep, and raise InterruptedError
    try:
        rtl_loop(config_radio, mock_mqtt, mock_proc_logic, "sys_id", "sys_model")
    except InterruptedError:
        pass # Expected exit
    
    # 5. Verify Results
    
    # Reading 1: Normal (123)
    calls = [c for c in mock_proc_logic.dispatch_reading.call_args_list if "123" in str(c)]
    assert len(calls) > 0 
    
    # Reading 2: SimpliSafe (999) - Should be blacklisted
    calls_blacklist = [c for c in mock_proc_logic.dispatch_reading.call_args_list if "999" in str(c)]
    assert len(calls_blacklist) == 0 
    
    # Reading 3: Temp F conversion (456)
    calls_f = [c for c in mock_proc_logic.dispatch_reading.call_args_list if "456" in str(c)]
    assert len(calls_f) > 0

    # Verify last_seen was injected and dispatched
    calls_last_seen = [c for c in mock_proc_logic.dispatch_reading.call_args_list if c.args[1] == "last_seen"]
    assert len(calls_last_seen) > 0

def test_flatten_nested_json():
    """Ensures nested JSON objects are flattened into single-level keys."""
    input_data = {
        "id": 123,
        "flags": ["foo", "bar"],
        "channel": {
            "A": 1,
            "B": 2
        }
    }
    
    expected = {
        "id": 123,
        "flags_0": "foo",
        "flags_1": "bar",
        "channel_A": 1,
        "channel_B": 2
    }
    
    assert flatten(input_data) == expected

def test_command_line_integrity(mocker):
    """
    CRITICAL: Ensures the rtl_433 command is built exactly as expected.
    Protects against AI accidentally removing flags like '-M level'.
    """
    # 1. Mock Popen so we don't actually start a process
    mock_popen = mocker.patch("subprocess.Popen")
    
    # Setup mocks to prevent the loop from running forever
    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.return_value = "" # EOF immediately
    mock_proc.poll.return_value = 1 # Process dead
    mock_popen.return_value = mock_proc
    
    # Mock sleep to break the loop instantly
    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError("Stop"))
    
    # 2. Define a Test Configuration
    radio_conf = {
        "name": "TestRadio",
        "id": "999",
        "freq": "915M, 433M", # Multiple Frequencies
        "hop_interval": 60,
        "rate": "1000k",
        "protocols": [1, 2]
    }
    
    # 3. Run the loop (it will crash/exit quickly due to mocks)
    try:
        rtl_loop(radio_conf, None, None, "sys_id", "sys_model")
    except InterruptedError:
        pass

    # 4. INSPECT THE COMMAND
    # Get the arguments passed to Popen
    args, _ = mock_popen.call_args
    cmd_list = args[0]
    
    # --- SAFETY CHECKS ---
    print(f"Generated Command: {cmd_list}")
    
    # Check for the specific flags you are worried about
    assert "-M" in cmd_list and "level" in cmd_list, "CRITICAL: '-M level' flag is missing!"
    assert "-F" in cmd_list and "json" in cmd_list, "CRITICAL: '-F json' flag is missing!"
    
    # Check Frequency Hopping
    assert "-f" in cmd_list
    assert "915M" in cmd_list
    assert "433M" in cmd_list
    assert "-H" in cmd_list and "60" in cmd_list
    
    # Check Sample Rate
    assert "-s" in cmd_list and "1000k" in cmd_list
    
    # Check Protocols
    assert "-R" in cmd_list and "1" in cmd_list
    assert "2" in cmd_list


def test_protocols_csv_parsing(mocker):
    """Protocols may come from the add-on UI as a comma-separated string."""
    mock_popen = mocker.patch("subprocess.Popen")

    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.return_value = ""  # EOF immediately
    mock_proc.poll.return_value = 1
    mock_popen.return_value = mock_proc

    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError("Stop"))

    radio_conf = {
        "name": "TestRadio",
        "id": "999",
        "freq": "915M",
        "rate": "250k",
        "protocols": "1, 2  ,3"
    }

    try:
        rtl_loop(radio_conf, None, None, "sys_id", "sys_model")
    except InterruptedError:
        pass

    args, _ = mock_popen.call_args
    cmd_list = args[0]

    assert "-R" in cmd_list
    assert "1" in cmd_list
    assert "2" in cmd_list
    assert "3" in cmd_list