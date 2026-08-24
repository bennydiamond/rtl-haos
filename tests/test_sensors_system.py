# tests/test_sensors_system.py
import pytest
import sensors_system
import io


def test_system_monitor_init_reads_model_file(mocker):
    # Make the devicetree model file exist and include nulls
    fake_model = "Raspberry Pi 4 Model B\x00"
    m = mocker.patch("builtins.open", return_value=io.StringIO(fake_model))
    mocker.patch.object(sensors_system.psutil, "boot_time", return_value=1000)
    mocker.patch.object(sensors_system.psutil, "Process", return_value=mocker.Mock())

    mon = sensors_system.SystemMonitor()
    assert mon.model_info == "Raspberry Pi 4 Model B"
    m.assert_called()


def test_system_monitor_read_stats_full_success(mocker):
    # stable init
    mocker.patch.object(sensors_system.psutil, "boot_time", return_value=1000)
    proc = mocker.Mock()
    proc.memory_info.return_value.rss = 50 * 1024 * 1024  # 50 MB
    mocker.patch.object(sensors_system.psutil, "Process", return_value=proc)
    mocker.patch("builtins.open", side_effect=FileNotFoundError)  # fall back to hostname
    mocker.patch.object(sensors_system.socket, "gethostname", return_value="host1")

    mon = sensors_system.SystemMonitor()

    mocker.patch.object(sensors_system.psutil, "cpu_percent", return_value=12.5)
    mocker.patch.object(
        sensors_system.psutil, "virtual_memory", return_value=mocker.Mock(percent=33.3)
    )
    mocker.patch.object(sensors_system.shutil, "disk_usage", return_value=(100, 25, 75))
    mocker.patch.object(
        sensors_system.psutil,
        "sensors_temperatures",
        return_value={"cpu_thermal": [mocker.Mock(current=55.0)]},
    )

    # fake outbound-IP socket
    sock = mocker.Mock()
    sock.getsockname.return_value = ("192.168.1.10", 12345)
    mocker.patch.object(sensors_system.socket, "socket", return_value=sock)

    stats = mon.read_stats()

    assert stats["sys_cpu"] == 12.5
    assert stats["sys_mem"] == 33.3
    assert stats["sys_script_mem"] == 50.0
    assert stats["sys_disk"] == 25.0
    assert stats["sys_temp"] == 55.0
    assert stats["sys_model"] == "host1"
    assert stats["sys_ip"] == "192.168.1.10"
    assert "sys_uptime" in stats


def test_system_monitor_read_stats_ip_fallback(mocker):
    mocker.patch.object(sensors_system.psutil, "boot_time", return_value=1000)
    mocker.patch.object(sensors_system.psutil, "Process", return_value=mocker.Mock())
    mocker.patch("builtins.open", side_effect=FileNotFoundError)

    mon = sensors_system.SystemMonitor()

    mocker.patch.object(sensors_system.socket, "socket", side_effect=OSError("no socket"))
    stats = mon.read_stats()
    assert stats["sys_ip"] == "127.0.0.1"
