import pytest
from rtl_manager import rtl_loop
from unittest.mock import MagicMock


def test_garbage_input_handling(mocker):
    """
    Ensures the system survives malformed JSON, binary garbage,
    and incomplete lines without crashing.
    """
    # 1. Setup
    mock_mqtt = mocker.Mock()
    mock_processor = mocker.Mock()

    # 2. Simulate a "Noisy" radio environment
    garbage_data = [
        "Plain text startup message",  # Not JSON
        "{ incomplete json ",  # Broken JSON
        '{"id": 1, "temp": }',  # Syntax Error
        b"\x00\x01\xff".decode("utf-8", errors="ignore"),  # Binary junk
        "",  # Empty line
        '{"model": "Survivor", "id": 1}',  # Valid data at the end
    ]

    # Mock the process output
    mock_proc = mocker.Mock()
    mock_proc.stdout.readline.side_effect = garbage_data + [""]
    mock_proc.poll.side_effect = [None] * len(garbage_data) + [1]

    mocker.patch("subprocess.Popen", return_value=mock_proc)
    mocker.patch("rtl_manager.time.sleep", side_effect=InterruptedError)

    # 3. Run Loop
    try:
        rtl_loop({"name": "Test"}, mock_mqtt, mock_processor, "sys", "mod")
    except InterruptedError:
        pass

    # 4. Verify Survival
    # The loop should have continued until it hit the valid line
    # We check if the valid line was processed.
    calls = mock_processor.dispatch_reading.call_args_list
    assert len(calls) > 0, "The valid message was skipped!"

    # Verify we extracted data from the "Survivor" device
    # Args: (clean_id, field, value, dev_name, model, ...)
    assert calls[0].args[4] == "Survivor"
