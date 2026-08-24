import rtl_manager


def test_safe_status_suffix_handles_empty_and_symbols():
    assert rtl_manager._safe_status_suffix(None) == "0"
    assert rtl_manager._safe_status_suffix("   ") == "0"
    assert rtl_manager._safe_status_suffix("a:b*c") == "a_b_c"


def test_discover_rtl_devices_missing_rtl_eeprom_does_not_crash(mocker, capsys):
    mocker.patch("rtl_manager.subprocess.run", side_effect=FileNotFoundError)

    devices = rtl_manager.discover_rtl_devices()
    out = capsys.readouterr().out.lower()

    assert devices == []
    assert "rtl_eeprom not found" in out


def test_discover_rtl_devices_uses_replace_errors_to_avoid_unicode_decode(mocker):
    """Regression guard: ensure we pass errors='replace' to subprocess.run.

    rtl_eeprom output may contain non-UTF8 bytes depending on dongle EEPROM contents.
    If we let subprocess decode with errors='strict' (default), the add-on can crash at
    startup with UnicodeDecodeError.
    """
    mock_run = mocker.patch("rtl_manager.subprocess.run")

    # Make discovery stop immediately.
    mock_run.return_value = mocker.Mock(
        stdout="No supported devices found.", stderr="", returncode=1
    )

    rtl_manager.discover_rtl_devices()

    _args, kwargs = mock_run.call_args
    assert kwargs.get("text") is True
    assert kwargs.get("errors") == "replace"
