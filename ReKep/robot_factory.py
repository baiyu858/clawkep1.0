from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from hardware_profile import HardwareProfile, build_hardware_profile, coerce_hardware_profile
from robot_adapter import RobotAdapter


def _create_dobot_adapter(
    *,
    driver: str,
    host: str | None,
    port: int | None,
    move_port: int | None,
    xtrainer_sdk_dir: str | Path | None,
) -> RobotAdapter:
    from dobot_bridge import (
        DashboardTcpDobotAdapter,
        MockDobotAdapter,
        XtrainerSdkDobotAdapter,
        XtrainerZmqDobotAdapter,
    )

    if driver == "mock":
        return MockDobotAdapter()
    if driver == "dashboard_tcp":
        return DashboardTcpDobotAdapter(host=host, port=port)
    if driver == "xtrainer_sdk":
        return XtrainerSdkDobotAdapter(
            sdk_dir=xtrainer_sdk_dir,
            robot_ip=host,
            dashboard_port=port,
            move_port=move_port,
        )
    if driver == "xtrainer_zmq":
        return XtrainerZmqDobotAdapter(host=host, port=port)
    raise RuntimeError(f"Unsupported dobot driver: {driver}")


def create_robot_adapter(
    *,
    hardware_profile: HardwareProfile | Dict[str, Any] | None = None,
    robot_family: str = "dobot",
    robot_driver: str | None = None,
    host: str | None = None,
    port: int | None = None,
    move_port: int | None = None,
    xtrainer_sdk_dir: str | Path | None = None,
) -> RobotAdapter:
    if hardware_profile is None:
        profile = build_hardware_profile(
            robot_family=robot_family,
            robot_driver=robot_driver or "xtrainer_zmq",
            robot_host=host,
            robot_port=port,
            robot_move_port=move_port,
            xtrainer_sdk_dir=str(xtrainer_sdk_dir or ""),
            camera_source="0",
            camera_profile="global3",
        )
    else:
        profile = coerce_hardware_profile(hardware_profile)

    if robot_family:
        profile.robot_family = str(robot_family).lower()
    if robot_driver:
        profile.robot_driver = str(robot_driver).lower()
    if host is not None:
        profile.robot_host = str(host)
    if port is not None:
        profile.robot_port = int(port)
    if move_port is not None:
        profile.robot_move_port = int(move_port)
    if xtrainer_sdk_dir is not None:
        profile.xtrainer_sdk_dir = str(Path(xtrainer_sdk_dir))

    family = str(profile.robot_family).lower()
    if family != "dobot":
        raise RuntimeError(f"Unsupported robot family: {profile.robot_family}")

    return _create_dobot_adapter(
        driver=profile.robot_driver,
        host=profile.robot_host,
        port=profile.robot_port,
        move_port=profile.robot_move_port,
        xtrainer_sdk_dir=profile.xtrainer_sdk_dir,
    )
