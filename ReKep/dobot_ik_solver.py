from __future__ import annotations

import math
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from types import SimpleNamespace
from typing import Iterable, Tuple

import numpy as np

_ROBOT_KINEMATICS_DIR = Path(__file__).resolve().parent / "robot" / "dobot" / "robot_kinematics"
if str(_ROBOT_KINEMATICS_DIR) not in sys.path:
    sys.path.append(str(_ROBOT_KINEMATICS_DIR))

from fk_nova2 import fk_dh_matrix


def _rotation_matrix_to_rotvec(rot: np.ndarray) -> np.ndarray:
    rot = np.asarray(rot, dtype=np.float64).reshape(3, 3)
    cos_theta = float((np.trace(rot) - 1.0) * 0.5)
    cos_theta = min(1.0, max(-1.0, cos_theta))
    theta = math.acos(cos_theta)
    if theta < 1e-9:
        return np.zeros(3, dtype=np.float64)
    sin_theta = math.sin(theta)
    if abs(sin_theta) < 1e-9:
        # Near pi: use diagonal terms as a stable axis estimate.
        axis = np.sqrt(np.maximum((np.diag(rot) + 1.0) * 0.5, 0.0))
        if float(np.linalg.norm(axis)) < 1e-9:
            axis = np.array([1.0, 0.0, 0.0], dtype=np.float64)
        axis = axis / max(float(np.linalg.norm(axis)), 1e-9)
        return axis * theta
    axis = np.array(
        [
            rot[2, 1] - rot[1, 2],
            rot[0, 2] - rot[2, 0],
            rot[1, 0] - rot[0, 1],
        ],
        dtype=np.float64,
    ) / (2.0 * sin_theta)
    return axis * theta


def _quat_xyzw_to_rot(q_xyzw: np.ndarray) -> np.ndarray:
    q = np.asarray(q_xyzw, dtype=np.float64).reshape(4)
    x, y, z, w = float(q[0]), float(q[1]), float(q[2]), float(q[3])
    n = x * x + y * y + z * z + w * w
    if n < 1e-12:
        return np.eye(3, dtype=np.float64)
    s = 2.0 / n
    xx, yy, zz = x * x * s, y * y * s, z * z * s
    xy, xz, yz = x * y * s, x * z * s, y * z * s
    wx, wy, wz = w * x * s, w * y * s, w * z * s
    return np.array(
        [
            [1.0 - (yy + zz), xy - wz, xz + wy],
            [xy + wz, 1.0 - (xx + zz), yz - wx],
            [xz - wy, yz + wx, 1.0 - (xx + yy)],
        ],
        dtype=np.float64,
    )


def _pose_quat_to_mat(pose_quat: np.ndarray) -> np.ndarray:
    pose = np.asarray(pose_quat, dtype=np.float64).reshape(-1)
    out = np.eye(4, dtype=np.float64)
    out[:3, :3] = _quat_xyzw_to_rot(pose[3:7])
    out[:3, 3] = pose[:3]
    return out


def _extract_joint_limits_from_urdf(urdf_path: Path) -> Tuple[np.ndarray, np.ndarray]:
    lower = np.full((6,), -2.0 * math.pi, dtype=np.float64)
    upper = np.full((6,), 2.0 * math.pi, dtype=np.float64)
    if not urdf_path.exists():
        return lower, upper
    try:
        root = ET.parse(str(urdf_path)).getroot()
    except Exception:
        return lower, upper
    for idx in range(6):
        jname = f"joint{idx + 1}"
        node = root.find(f"./joint[@name='{jname}']")
        if node is None:
            continue
        limit_node = node.find("limit")
        if limit_node is None:
            continue
        try:
            lo = float(limit_node.attrib.get("lower", lower[idx]))
            hi = float(limit_node.attrib.get("upper", upper[idx]))
        except Exception:
            continue
        if math.isfinite(lo) and math.isfinite(hi) and hi > lo:
            lower[idx] = lo
            upper[idx] = hi
    return lower, upper


def _as_joint_seed(values: Iterable[float] | None, *, arm: str) -> np.ndarray | None:
    if values is None:
        return None
    arr = np.asarray(list(values), dtype=np.float64).reshape(-1)
    if arr.size >= 14:
        # xtrainer convention: [left7, right7]
        if str(arm).strip().lower() == "left":
            arr = arr[:7]
        else:
            arr = arr[7:14]
    if arr.size >= 7:
        arr = arr[:6]
    elif arr.size >= 6:
        arr = arr[:6]
    else:
        return None
    if np.max(np.abs(arr)) > 7.0:
        arr = np.deg2rad(arr)
    return arr.astype(np.float64, copy=False)


class DobotNova2IKSolver:
    """Numerical IK wrapper around Dobot nova2 kinematics (DH + URDF limits)."""

    def __init__(
        self,
        *,
        bounds_min: np.ndarray,
        bounds_max: np.ndarray,
        arm: str = "right",
        tool_z_m: float = 0.2,
        tool_y_m: float = 0.0,
        max_iterations: int = 10,
        damping: float = 0.08,
        max_joint_step_rad: float = 0.25,
        fd_eps_rad: float = 1e-4,
    ):
        self.bounds_min = np.asarray(bounds_min, dtype=np.float64)
        self.bounds_max = np.asarray(bounds_max, dtype=np.float64)
        self.arm = str(arm or "right").strip().lower()
        self.tool_z_m = float(tool_z_m)
        self.tool_y_m = float(tool_y_m)
        self.max_iterations = max(1, int(max_iterations))
        self.damping = max(1e-5, float(damping))
        self.max_joint_step_rad = max(1e-3, float(max_joint_step_rad))
        self.fd_eps_rad = max(1e-6, float(fd_eps_rad))
        self.joint_dim = 6

        urdf_path = _ROBOT_KINEMATICS_DIR / "nova2_robot.urdf"
        self.joint_lower, self.joint_upper = _extract_joint_limits_from_urdf(urdf_path)
        self.neutral_q = np.clip(np.zeros((6,), dtype=np.float64), self.joint_lower, self.joint_upper)
        self._last_success_q = self.neutral_q.copy()
        self.runtime_from_model = np.eye(4, dtype=np.float64)
        self.model_from_runtime = np.eye(4, dtype=np.float64)

    def _fk(self, q_rad: np.ndarray) -> np.ndarray:
        q = np.asarray(q_rad, dtype=np.float64).reshape(6)
        return fk_dh_matrix(
            float(q[0]),
            float(q[1]),
            float(q[2]),
            float(q[3]),
            float(q[4]),
            float(q[5]),
            self.tool_y_m,
            self.tool_z_m,
        ).astype(np.float64)

    def _seed(self, initial_joint_pos=None) -> np.ndarray:
        seed = _as_joint_seed(initial_joint_pos, arm=self.arm)
        if seed is None:
            seed = self._last_success_q.copy()
        return np.clip(seed, self.joint_lower, self.joint_upper)

    def _within_workspace(self, pos: np.ndarray) -> bool:
        return bool(np.all(pos >= self.bounds_min) and np.all(pos <= self.bounds_max))

    def set_runtime_alignment(self, *, joint_seed_rad: Iterable[float] | None, runtime_pose_quat: np.ndarray | None) -> bool:
        seed = _as_joint_seed(joint_seed_rad, arm=self.arm)
        runtime_pose = np.asarray(runtime_pose_quat, dtype=np.float64).reshape(-1) if runtime_pose_quat is not None else np.zeros((0,), dtype=np.float64)
        if seed is None or runtime_pose.size < 7:
            return False
        try:
            t_model = self._fk(seed)
            t_runtime = _pose_quat_to_mat(runtime_pose)
            runtime_from_model = t_runtime @ np.linalg.inv(t_model)
            self.runtime_from_model = runtime_from_model
            self.model_from_runtime = np.linalg.inv(runtime_from_model)
            self._last_success_q = np.clip(seed, self.joint_lower, self.joint_upper)
            return True
        except Exception:
            return False

    def _solve_dls(
        self,
        *,
        target_pos: np.ndarray,
        target_rot: np.ndarray,
        q_seed: np.ndarray,
        position_weight: float,
        orientation_weight: float,
        iter_budget: int,
    ) -> Tuple[np.ndarray, float, float, int]:
        q = np.clip(np.asarray(q_seed, dtype=np.float64).reshape(6), self.joint_lower, self.joint_upper)
        i_eye = np.eye(6, dtype=np.float64)

        last_pos_err = float("inf")
        last_rot_err = float("inf")
        for step in range(1, iter_budget + 1):
            t_cur = self._fk(q)
            pos_cur = t_cur[:3, 3]
            rot_cur = t_cur[:3, :3]
            pos_err_vec = target_pos - pos_cur
            rot_err_vec = _rotation_matrix_to_rotvec(target_rot @ rot_cur.T)
            pos_err = float(np.linalg.norm(pos_err_vec))
            rot_err = float(np.linalg.norm(rot_err_vec))
            last_pos_err = pos_err
            last_rot_err = rot_err

            weighted_err = np.concatenate(
                [
                    float(position_weight) * pos_err_vec,
                    float(orientation_weight) * rot_err_vec,
                ],
                axis=0,
            )
            if float(np.linalg.norm(weighted_err)) < 1e-6:
                return q, last_pos_err, last_rot_err, step

            jac = np.zeros((6, 6), dtype=np.float64)
            for jid in range(6):
                q_perturbed = q.copy()
                q_perturbed[jid] = np.clip(q_perturbed[jid] + self.fd_eps_rad, self.joint_lower[jid], self.joint_upper[jid])
                t_eps = self._fk(q_perturbed)
                pos_eps = t_eps[:3, 3]
                rot_eps = t_eps[:3, :3]
                jac[:3, jid] = (pos_eps - pos_cur) / self.fd_eps_rad
                jac[3:, jid] = _rotation_matrix_to_rotvec(rot_eps @ rot_cur.T) / self.fd_eps_rad

            wjac = jac.copy()
            wjac[:3, :] *= float(position_weight)
            wjac[3:, :] *= float(orientation_weight)
            jj_t = wjac @ wjac.T
            damped = jj_t + (self.damping**2) * i_eye
            try:
                dq = wjac.T @ np.linalg.solve(damped, weighted_err)
            except np.linalg.LinAlgError:
                dq = wjac.T @ np.linalg.pinv(damped) @ weighted_err
            dq = np.clip(dq, -self.max_joint_step_rad, self.max_joint_step_rad)
            q = np.clip(q + dq, self.joint_lower, self.joint_upper)

        return q, last_pos_err, last_rot_err, iter_budget

    def solve(
        self,
        target_pose_homo,
        position_tolerance=0.01,
        orientation_tolerance=0.05,
        position_weight=1.0,
        orientation_weight=0.05,
        max_iterations=150,
        initial_joint_pos=None,
    ):
        target_pose_homo = np.asarray(target_pose_homo, dtype=np.float64).reshape(4, 4)
        target_pos_runtime = target_pose_homo[:3, 3]
        requested_iters = max(1, int(max_iterations))
        iter_budget = min(self.max_iterations, requested_iters)

        # Hard reject for explicit workspace box mismatch.
        if not self._within_workspace(target_pos_runtime):
            clipped = np.clip(target_pos_runtime, self.bounds_min, self.bounds_max)
            return SimpleNamespace(
                success=False,
                position_error=float(np.linalg.norm(target_pos_runtime - clipped)),
                orientation_error=0.0,
                num_descents=iter_budget,
                cspace_position=self._seed(initial_joint_pos),
            )

        target_pose_model = self.model_from_runtime @ target_pose_homo
        target_pos = target_pose_model[:3, 3]
        target_rot = target_pose_model[:3, :3]
        q_seed = self._seed(initial_joint_pos)
        q_sol, pos_err, rot_err, num_descents = self._solve_dls(
            target_pos=target_pos,
            target_rot=target_rot,
            q_seed=q_seed,
            position_weight=float(position_weight),
            orientation_weight=float(orientation_weight),
            iter_budget=iter_budget,
        )

        # One cheap fallback from neutral seed if local minimum is poor.
        if pos_err > max(float(position_tolerance), 0.015):
            q_fallback, pos_err_fb, rot_err_fb, desc_fb = self._solve_dls(
                target_pos=target_pos,
                target_rot=target_rot,
                q_seed=self.neutral_q,
                position_weight=float(position_weight),
                orientation_weight=float(orientation_weight),
                iter_budget=max(2, iter_budget // 2),
            )
            if pos_err_fb < pos_err:
                q_sol = q_fallback
                pos_err = pos_err_fb
                rot_err = rot_err_fb
            num_descents = min(requested_iters, int(num_descents + desc_fb))

        solved_model = self._fk(q_sol)
        solved_runtime = self.runtime_from_model @ solved_model
        runtime_pos_err = float(np.linalg.norm(target_pose_homo[:3, 3] - solved_runtime[:3, 3]))
        runtime_rot_err = float(np.linalg.norm(_rotation_matrix_to_rotvec(target_pose_homo[:3, :3] @ solved_runtime[:3, :3].T)))

        success = bool(runtime_pos_err <= float(position_tolerance) and runtime_rot_err <= float(orientation_tolerance))
        if success:
            self._last_success_q = q_sol.copy()

        return SimpleNamespace(
            success=success,
            position_error=float(runtime_pos_err),
            orientation_error=float(runtime_rot_err),
            num_descents=int(min(requested_iters, max(0, num_descents))),
            cspace_position=np.asarray(q_sol, dtype=np.float64),
        )
