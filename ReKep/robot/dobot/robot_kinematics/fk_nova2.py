from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal, Sequence, Tuple

import numpy as np

ToolYMode = Literal["zero", "claw", "+claw", "-claw"]


def dh_transformation_matrix(theta: float, d: float, a: float, alpha: float) -> np.ndarray:
    cos_theta = math.cos(theta)
    sin_theta = math.sin(theta)
    cos_alpha = math.cos(alpha)
    sin_alpha = math.sin(alpha)
    return np.array(
        [
            [cos_theta, -sin_theta * cos_alpha, sin_theta * sin_alpha, a * cos_theta],
            [sin_theta, cos_theta * cos_alpha, -cos_theta * sin_alpha, a * sin_theta],
            [0.0, sin_alpha, cos_alpha, d],
            [0.0, 0.0, 0.0, 1.0],
        ],
        dtype=float,
    )


def claw_width(coef: float) -> float:
    claw_servo = 2.3818 - float(coef) * 1.5401
    cos_claw_servo = math.cos(claw_servo)
    return 0.03 * cos_claw_servo + 0.5 * math.sqrt(0.0036 * cos_claw_servo**2 + 0.0028)


def fk_dh_matrix(
    q0: float,
    q1: float,
    q2: float,
    q3: float,
    q4: float,
    q5: float,
    tool_y: float,
    tool_z: float,
) -> np.ndarray:
    dh_params = [
        (q0, 0.2234, 0.0, math.pi / 2.0),
        (q1 - math.pi / 2.0, 0.0, -0.280, 0.0),
        (q2, 0.0, -0.225, 0.0),
        (q3 - math.pi / 2.0, 0.1175, 0.0, math.pi / 2.0),
        (q4, 0.120, 0.0, -math.pi / 2.0),
        (q5, 0.088, 0.0, 0.0),
    ]

    t = np.eye(4, dtype=float)
    for params in dh_params:
        t = t @ dh_transformation_matrix(*params)

    t_tool = np.eye(4, dtype=float)
    t_tool[:3, 3] = np.array([0.0, float(tool_y), float(tool_z)], dtype=float)
    return t @ t_tool


def rpy_xyz_from_rot(R: np.ndarray) -> np.ndarray:
    R = np.asarray(R, dtype=float).reshape(3, 3)
    pitch = math.atan2(-float(R[2, 0]), math.sqrt(float(R[2, 1]) ** 2 + float(R[2, 2]) ** 2))
    if abs(abs(pitch) - (math.pi / 2.0)) < 1e-10:
        roll = 0.0
        yaw = math.atan2(-float(R[0, 1]), float(R[1, 1]))
    else:
        roll = math.atan2(float(R[2, 1]), float(R[2, 2]))
        yaw = math.atan2(float(R[1, 0]), float(R[0, 0]))
    return np.array([roll, pitch, yaw], dtype=float)


def compute_ee_xyzrxryrz_deg_mm(
    joint_positions_14: np.ndarray,
    *,
    tool_z_m: float = 0.2,
    tool_y_mode: ToolYMode = "zero",
) -> np.ndarray:
    joint_positions_14 = np.asarray(joint_positions_14, dtype=float).reshape(14)

    def tool_y(side: Literal["left", "right"]) -> float:
        if tool_y_mode == "zero":
            return 0.0
        coef = joint_positions_14[6] if side == "left" else joint_positions_14[13]
        if tool_y_mode == "claw":
            return claw_width(coef)
        if tool_y_mode == "+claw":
            return abs(claw_width(coef))
        if tool_y_mode == "-claw":
            return -abs(claw_width(coef))
        raise ValueError(f"Unknown tool_y_mode: {tool_y_mode!r}")

    T_left = fk_dh_matrix(*joint_positions_14[0:6], tool_y("left"), tool_z_m)
    T_right = fk_dh_matrix(*joint_positions_14[7:13], tool_y("right"), tool_z_m)

    xyz_left_mm = T_left[:3, 3] * 1000.0
    xyz_right_mm = T_right[:3, 3] * 1000.0
    rpy_left_deg = np.degrees(rpy_xyz_from_rot(T_left[:3, :3]))
    rpy_right_deg = np.degrees(rpy_xyz_from_rot(T_right[:3, :3]))

    return np.concatenate([xyz_left_mm, rpy_left_deg, xyz_right_mm, rpy_right_deg]).astype(np.float32)


@dataclass(frozen=True)
class PoseErrors:
    left_pos_err_mm: float
    right_pos_err_mm: float
    left_rot_err_deg: float
    right_rot_err_deg: float


def rot_from_rpy_xyz(rpy_rad: Sequence[float]) -> np.ndarray:
    roll, pitch, yaw = (float(rpy_rad[0]), float(rpy_rad[1]), float(rpy_rad[2]))
    cr, sr = math.cos(roll), math.sin(roll)
    cp, sp = math.cos(pitch), math.sin(pitch)
    cy, sy = math.cos(yaw), math.sin(yaw)
    return np.array(
        [
            [cy * cp, cy * sp * sr - sy * cr, cy * sp * cr + sy * sr],
            [sy * cp, sy * sp * sr + cy * cr, sy * sp * cr - cy * sr],
            [-sp, cp * sr, cp * cr],
        ],
        dtype=float,
    )


def geodesic_angle_deg(R_a: np.ndarray, R_b: np.ndarray) -> float:
    R_a = np.asarray(R_a, dtype=float).reshape(3, 3)
    R_b = np.asarray(R_b, dtype=float).reshape(3, 3)
    R = R_a.T @ R_b
    c = (float(np.trace(R)) - 1.0) / 2.0
    c = max(-1.0, min(1.0, c))
    return math.degrees(math.acos(c))


def pose_errors(pred_12: np.ndarray, gt_12: np.ndarray) -> PoseErrors:
    pred_12 = np.asarray(pred_12, dtype=float).reshape(12)
    gt_12 = np.asarray(gt_12, dtype=float).reshape(12)

    def per_arm(i0: int) -> Tuple[float, float]:
        pred_xyz = pred_12[i0 : i0 + 3]
        gt_xyz = gt_12[i0 : i0 + 3]
        pos_err_mm = float(np.linalg.norm(pred_xyz - gt_xyz))

        pred_rpy = np.radians(pred_12[i0 + 3 : i0 + 6])
        gt_rpy = np.radians(gt_12[i0 + 3 : i0 + 6])
        R_pred = rot_from_rpy_xyz(pred_rpy)
        R_gt = rot_from_rpy_xyz(gt_rpy)
        rot_err_deg = geodesic_angle_deg(R_gt, R_pred)
        return pos_err_mm, rot_err_deg

    left_pos, left_rot = per_arm(0)
    right_pos, right_rot = per_arm(6)
    return PoseErrors(
        left_pos_err_mm=left_pos,
        right_pos_err_mm=right_pos,
        left_rot_err_deg=left_rot,
        right_rot_err_deg=right_rot,
    )

