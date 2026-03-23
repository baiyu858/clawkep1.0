from __future__ import annotations

import copy
import json
import math
import os
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple

import cv2
import numpy as np

import transform_utils as T
from dobot_ik_solver import DobotNova2IKSolver
from keypoint_proposal import KeypointProposer
from path_solver import PathSolver
from subgoal_solver import SubgoalSolver
from utils import get_config, get_linear_interpolation_steps, load_functions_from_txt

# Right-arm safe zone adapted from dobot_xtrainer_remote/experiments/run_control.py.
RIGHT_ARM_BOUNDS_MIN_M = np.array([-0.25, -0.75, 0.04], dtype=np.float64)
RIGHT_ARM_BOUNDS_MAX_M = np.array([0.45, -0.16, 0.45], dtype=np.float64)
LEFT_ARM_BOUNDS_MIN_M = np.array([-0.45, -0.75, 0.04], dtype=np.float64)
LEFT_ARM_BOUNDS_MAX_M = np.array([0.30, -0.16, 0.45], dtype=np.float64)
DEFAULT_REAL_GRASP_DEPTH_M = 0.03
DEFAULT_RELEASE_OPEN_WAIT_S = 0.15
DEFAULT_POST_GRASP_LIFT_M = 0.08
DEFAULT_PRE_RELEASE_HOVER_M = 0.06
DEFAULT_PRE_RELEASE_DESCEND_M = 0.015
DEFAULT_POST_RELEASE_RETREAT_M = 0.08
DEFAULT_MAX_MOVEL_STEP_M = 0.06
DEFAULT_MAX_MOVEL_ROT_STEP_DEG = 15.0
DEFAULT_MIN_MOVEL_STEP_M = 0.005
DEFAULT_MIN_MOVEL_ROT_STEP_DEG = 3.0
_SAM_MASK_GENERATOR_CACHE: Dict[Tuple[str, str, str, int, float, float, int, int], Any] = {}
_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SAM_CANDIDATES = (
    ("vit_h", _REPO_ROOT / ".downloads/models/sam/sam_vit_h_4b8939.pth"),
    ("vit_l", _REPO_ROOT / ".downloads/models/sam/sam_vit_l_0b3195.pth"),
    ("vit_b", _REPO_ROOT / ".downloads/models/sam/sam_vit_b_01ec64.pth"),
)
DEFAULT_TOOL_COLLISION_LOCAL_POINTS_M = np.array(
    [
        [0.000, 0.000, -0.030],
        [0.018, 0.018, -0.010],
        [-0.018, 0.018, -0.010],
        [0.018, -0.018, -0.010],
        [-0.018, -0.018, -0.010],
        [0.014, 0.012, 0.015],
        [-0.014, 0.012, 0.015],
        [0.014, -0.012, 0.015],
        [-0.014, -0.012, 0.015],
        [0.010, 0.000, 0.040],
        [-0.010, 0.000, 0.040],
        [0.000, 0.008, 0.050],
        [0.000, -0.008, 0.050],
    ],
    dtype=np.float64,
)


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    return value


def _normalized_axis(vector: Any, fallback: np.ndarray) -> np.ndarray:
    arr = np.asarray(vector, dtype=np.float64).reshape(-1)
    if arr.size < 3:
        arr = np.asarray(fallback, dtype=np.float64).reshape(-1)
    arr = arr[:3]
    norm = np.linalg.norm(arr)
    if norm < 1e-9:
        arr = np.asarray(fallback, dtype=np.float64).reshape(-1)[:3]
        norm = np.linalg.norm(arr)
    return arr / max(norm, 1e-9)


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    value = str(raw).strip().lower()
    if value in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "f", "no", "n", "off"}:
        return False
    return bool(default)


def _read_int_env(name: str, default: int, lower: int, upper: int) -> int:
    raw = os.environ.get(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except Exception:
        value = int(default)
    return max(int(lower), min(int(upper), int(value)))


def _read_float_env(name: str, default: float, lower: float, upper: float) -> float:
    raw = os.environ.get(name)
    try:
        value = float(raw) if raw is not None else float(default)
    except Exception:
        value = float(default)
    return max(float(lower), min(float(upper), float(value)))


def _resolve_default_sam_asset() -> Tuple[str, Path | None]:
    for model_type, checkpoint_path in _DEFAULT_SAM_CANDIDATES:
        if checkpoint_path.exists():
            return model_type, checkpoint_path
    return "", None


def _infer_sam_model_type(checkpoint_path: str | Path, fallback: str = "vit_b") -> str:
    name = Path(checkpoint_path).name.lower()
    if "vit_h" in name:
        return "vit_h"
    if "vit_l" in name:
        return "vit_l"
    if "vit_b" in name:
        return "vit_b"
    return str(fallback or "vit_b")


def _get_solver_sam_mask_generator():
    use_sam = _read_bool_env("REKEP_SOLVER_USE_SAM", True)
    if not use_sam:
        raise RuntimeError("solver candidate proposal now requires SAM; do not disable REKEP_SOLVER_USE_SAM")

    default_model_type, default_checkpoint_path = _resolve_default_sam_asset()
    checkpoint = str(os.environ.get("REKEP_SOLVER_SAM_CHECKPOINT", "")).strip()
    if not checkpoint:
        checkpoint = str(os.environ.get("REKEP_KEYPOINT_FINE_SAM_CHECKPOINT", "")).strip()
    checkpoint_path = Path(checkpoint) if checkpoint else default_checkpoint_path
    if checkpoint_path is None or not checkpoint_path.exists():
        raise RuntimeError(
            "SAM checkpoint not found for solver candidate proposal. "
            "Set REKEP_SOLVER_SAM_CHECKPOINT or place a checkpoint under .downloads/models/sam/."
        )

    model_type = str(os.environ.get("REKEP_SOLVER_SAM_MODEL_TYPE", "")).strip()
    if not model_type:
        model_type = str(os.environ.get("REKEP_KEYPOINT_FINE_SAM_MODEL_TYPE", "")).strip()
    if not model_type:
        model_type = default_model_type or _infer_sam_model_type(checkpoint_path, "vit_b")

    device = str(os.environ.get("REKEP_SOLVER_SAM_DEVICE", "")).strip()
    if not device:
        device = str(os.environ.get("REKEP_KEYPOINT_FINE_SAM_DEVICE", "")).strip()
    if not device:
        try:
            import torch

            device = "cuda" if torch.cuda.is_available() else "cpu"
        except Exception:
            device = "cpu"

    points_per_side = _read_int_env("REKEP_SOLVER_SAM_POINTS_PER_SIDE", 24, 4, 128)
    pred_iou_thresh = _read_float_env("REKEP_SOLVER_SAM_PRED_IOU_THRESH", 0.86, 0.0, 1.0)
    stability_score_thresh = _read_float_env("REKEP_SOLVER_SAM_STABILITY_THRESH", 0.92, 0.0, 1.0)
    crop_n_layers = _read_int_env("REKEP_SOLVER_SAM_CROP_N_LAYERS", 1, 0, 3)
    min_mask_region_area = _read_int_env("REKEP_SOLVER_SAM_MIN_REGION_AREA", 200, 0, 20000)

    cache_key = (
        str(checkpoint_path.resolve()),
        str(model_type),
        str(device),
        int(points_per_side),
        float(pred_iou_thresh),
        float(stability_score_thresh),
        int(crop_n_layers),
        int(min_mask_region_area),
    )
    if cache_key in _SAM_MASK_GENERATOR_CACHE:
        return _SAM_MASK_GENERATOR_CACHE[cache_key]

    try:
        from segment_anything import SamAutomaticMaskGenerator, sam_model_registry
    except Exception as exc:
        raise RuntimeError(
            "segment_anything is not installed in the rekep environment; install it before using solver mode"
        ) from exc

    try:
        sam_model = sam_model_registry[model_type](checkpoint=str(checkpoint_path))
        sam_model.to(device=device)
    except Exception as exc:
        raise RuntimeError(f"failed to initialize SAM model for solver mode: {exc}") from exc

    generator = SamAutomaticMaskGenerator(
        model=sam_model,
        points_per_side=int(points_per_side),
        pred_iou_thresh=float(pred_iou_thresh),
        stability_score_thresh=float(stability_score_thresh),
        crop_n_layers=int(crop_n_layers),
        min_mask_region_area=int(min_mask_region_area),
    )
    _SAM_MASK_GENERATOR_CACHE[cache_key] = generator
    return generator


class DummyIKSolver:
    """Cheap reachability stub for the real Dobot path.

    The official ReKep solver expects an IK object with Lula-like fields. We keep the
    same interface but only enforce workspace bounds, since live Dobot execution uses
    Cartesian MovL directly.
    """

    def __init__(self, *, bounds_min: np.ndarray, bounds_max: np.ndarray, joint_dim: int = 6):
        self.bounds_min = np.asarray(bounds_min, dtype=np.float64)
        self.bounds_max = np.asarray(bounds_max, dtype=np.float64)
        self.joint_dim = int(joint_dim)

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
        target_pose_homo = np.asarray(target_pose_homo, dtype=np.float64)
        pos = target_pose_homo[:3, 3]
        success = bool(np.all(pos >= self.bounds_min) and np.all(pos <= self.bounds_max))
        cspace_dim = max(self.joint_dim, len(initial_joint_pos) if initial_joint_pos is not None else self.joint_dim)
        cspace = np.zeros(int(cspace_dim), dtype=np.float64)
        if initial_joint_pos is not None:
            seed = np.asarray(initial_joint_pos, dtype=np.float64).flatten()
            cspace[: min(len(seed), len(cspace))] = seed[: min(len(seed), len(cspace))]
        return SimpleNamespace(
            success=success,
            position_error=0.0 if success else float(np.linalg.norm(np.clip(pos, self.bounds_min, self.bounds_max) - pos)),
            num_descents=0 if success else max(1, int(max_iterations)),
            cspace_position=cspace,
        )


class RealSolverContext:
    def __init__(self, *, config: Dict[str, Any], adapter, emit_progress, arm: str = "right"):
        self.config = config
        self.adapter = adapter
        self.emit_progress = emit_progress
        self.bounds_min = np.asarray(self.config["subgoal_solver"]["bounds_min"], dtype=np.float64)
        self.bounds_max = np.asarray(self.config["subgoal_solver"]["bounds_max"], dtype=np.float64)
        self.active_arm = str(arm or "right").strip().lower()
        self.reset_joint_pos = self._get_current_joint_seed(self.active_arm)
        self.ik_solver = self._build_ik_solver(self.active_arm)
        self.subgoal_solver = SubgoalSolver(self.config["subgoal_solver"], self.ik_solver, self.reset_joint_pos)
        self.path_solver = PathSolver(self.config["path_solver"], self.ik_solver, self.reset_joint_pos)
        self.sdf_voxels = np.zeros((24, 24, 24), dtype=np.float32)
        self.current_ee_pose = None
        self.scene_keypoints = None
        self.rigid_group_ids = None
        self.attached_group = None
        self.attached_local_points: Dict[int, np.ndarray] = {}
        self.grasped_keypoints: set[int] = set()

    def _build_ik_solver(self, arm: str):
        main_cfg = self.config.get("main", {}) if isinstance(self.config, dict) else {}
        tool_z = float(main_cfg.get("ik_tool_z_m", 0.2))
        tool_y = float(main_cfg.get("ik_tool_y_m", 0.0))
        try:
            return DobotNova2IKSolver(
                bounds_min=self.bounds_min,
                bounds_max=self.bounds_max,
                arm=arm,
                tool_z_m=tool_z,
                tool_y_m=tool_y,
                max_iterations=int(main_cfg.get("ik_max_iterations", 10)),
                damping=float(main_cfg.get("ik_damping", 0.08)),
                max_joint_step_rad=float(main_cfg.get("ik_max_joint_step_rad", 0.25)),
            )
        except Exception as exc:
            self.emit_progress(f"[rekep-solver] Dobot IK init failed, fallback to workspace IK: {exc}")
            return DummyIKSolver(bounds_min=self.bounds_min, bounds_max=self.bounds_max)

    def _get_current_joint_seed(self, arm: str) -> np.ndarray:
        # Seed from current robot state when available; keep 6DOF arm joints only.
        try:
            runtime_state = self.adapter.get_runtime_state() if hasattr(self.adapter, "get_runtime_state") else {}
        except Exception:
            runtime_state = {}
        joint_state = np.asarray((runtime_state or {}).get("joint_state", []), dtype=np.float64).reshape(-1)
        if joint_state.size >= 14:
            if str(arm).strip().lower() == "left":
                joint_state = joint_state[:6]
            else:
                joint_state = joint_state[7:13]
        elif joint_state.size >= 7:
            joint_state = joint_state[:6]
        elif joint_state.size >= 6:
            joint_state = joint_state[:6]
        else:
            joint_state = np.zeros((6,), dtype=np.float64)
        if np.max(np.abs(joint_state)) > 7.0:
            joint_state = np.deg2rad(joint_state)
        if joint_state.shape[0] != 6:
            seed = np.zeros((6,), dtype=np.float64)
            seed[: min(6, int(joint_state.shape[0]))] = joint_state[: min(6, int(joint_state.shape[0]))]
            joint_state = seed
        return np.asarray(joint_state, dtype=np.float64)

    def set_initial_scene(self, keypoints_3d: Dict[str, List[float]], rigid_group_ids: Dict[str, int], *, arm: str = "right"):
        ordered_ids = sorted(int(k) for k in keypoints_3d.keys())
        self.scene_keypoints = np.asarray([keypoints_3d[str(idx)] for idx in ordered_ids], dtype=np.float64)
        self.rigid_group_ids = np.asarray([int(rigid_group_ids.get(str(idx), -1)) for idx in ordered_ids], dtype=np.int32)
        self.active_arm = arm
        self.reset_joint_pos = self._get_current_joint_seed(self.active_arm)
        if hasattr(self.ik_solver, "arm"):
            self.ik_solver.arm = self.active_arm
        self.current_ee_pose = self._get_current_ee_pose(arm)
        if hasattr(self.ik_solver, "set_runtime_alignment"):
            aligned = bool(
                self.ik_solver.set_runtime_alignment(
                    joint_seed_rad=self.reset_joint_pos,
                    runtime_pose_quat=self.current_ee_pose,
                )
            )
            if aligned:
                self.emit_progress("[rekep-solver] Dobot IK aligned to current runtime frame")
            else:
                self.emit_progress("[rekep-solver] Dobot IK alignment skipped; using default model frame")

    def current_joint_seed(self) -> np.ndarray:
        self.reset_joint_pos = self._get_current_joint_seed(self.active_arm)
        return np.asarray(self.reset_joint_pos, dtype=np.float64)

    def _get_current_ee_pose(self, arm: str) -> np.ndarray:
        if hasattr(self.adapter, "get_tool_pose_mm_deg"):
            raw = self.adapter.get_tool_pose_mm_deg(arm)
            if isinstance(raw, (list, tuple)) and len(raw) >= 6:
                pos_m = np.asarray(raw[:3], dtype=np.float64) / 1000.0
                quat = T.euler2quat(np.deg2rad(np.asarray(raw[3:6], dtype=np.float64)))
                return np.concatenate([pos_m, quat], axis=0)
        # Conservative fallback used only if remote pose query is unavailable.
        return np.array([0.18, -0.42, 0.18, *T.euler2quat(np.deg2rad(np.array([180.0, 0.0, 0.0])))], dtype=np.float64)

    def refresh_attached_world_keypoints(self):
        if self.scene_keypoints is None or not self.attached_local_points or self.current_ee_pose is None:
            return
        ee_homo = T.pose2mat([self.current_ee_pose[:3], self.current_ee_pose[3:]])
        for idx, local_point in self.attached_local_points.items():
            world = ee_homo @ np.array([float(local_point[0]), float(local_point[1]), float(local_point[2]), 1.0], dtype=np.float64)
            self.scene_keypoints[idx] = world[:3]

    def movable_mask(self) -> np.ndarray:
        mask = np.zeros((len(self.scene_keypoints) + 1,), dtype=bool)
        mask[0] = True
        if self.attached_group is None:
            return mask
        mask[1:] = self.rigid_group_ids == int(self.attached_group)
        return mask

    def full_keypoints(self) -> np.ndarray:
        self.refresh_attached_world_keypoints()
        return np.concatenate([self.current_ee_pose[:3][None], self.scene_keypoints], axis=0)

    def current_collision_points(self) -> np.ndarray:
        if self.current_ee_pose is None:
            return np.zeros((0, 3), dtype=np.float64)
        ee_homo = T.pose2mat([self.current_ee_pose[:3], self.current_ee_pose[3:]])
        tool_points = (
            DEFAULT_TOOL_COLLISION_LOCAL_POINTS_M @ ee_homo[:3, :3].T
        ) + ee_homo[:3, 3]
        attached_points = []
        if self.attached_local_points:
            self.refresh_attached_world_keypoints()
            for idx in sorted(self.attached_local_points.keys()):
                attached_points.append(self.scene_keypoints[int(idx)])
        if attached_points:
            return np.vstack([tool_points, np.asarray(attached_points, dtype=np.float64)])
        return tool_points

    def mark_grasped_group(self, grasp_keypoint: int):
        if grasp_keypoint < 0:
            return
        self.refresh_attached_world_keypoints()
        group = int(self.rigid_group_ids[grasp_keypoint])
        self.attached_group = group
        ee_homo_inv = np.linalg.inv(T.pose2mat([self.current_ee_pose[:3], self.current_ee_pose[3:]]))
        self.attached_local_points = {}
        for idx, group_id in enumerate(self.rigid_group_ids):
            if int(group_id) != group:
                continue
            point = np.append(self.scene_keypoints[idx], 1.0)
            local_point = ee_homo_inv @ point
            self.attached_local_points[idx] = local_point[:3]
        self.grasped_keypoints.add(int(grasp_keypoint))

    def clear_grasp(self):
        self.refresh_attached_world_keypoints()
        self.attached_group = None
        self.attached_local_points = {}
        self.grasped_keypoints.clear()



def build_real_solver_config(*, arm: str = "right", grasp_depth_m: float = DEFAULT_REAL_GRASP_DEPTH_M) -> Dict[str, Any]:
    config = copy.deepcopy(get_config())
    arm_name = str(arm or "right").strip().lower()
    if arm_name == "left":
        bounds_min = LEFT_ARM_BOUNDS_MIN_M.copy()
        bounds_max = LEFT_ARM_BOUNDS_MAX_M.copy()
    else:
        bounds_min = RIGHT_ARM_BOUNDS_MIN_M.copy()
        bounds_max = RIGHT_ARM_BOUNDS_MAX_M.copy()

    for section in ("main", "path_solver", "subgoal_solver", "keypoint_proposer", "visualizer"):
        if section not in config:
            continue
        config[section]["bounds_min"] = bounds_min.copy()
        config[section]["bounds_max"] = bounds_max.copy()

    config["main"]["grasp_depth"] = float(grasp_depth_m)
    config["main"]["grasp_retry_backoff"] = 0.012
    config["main"]["grasp_retry_settle_time"] = 0.15
    config["main"]["post_grasp_lift"] = DEFAULT_POST_GRASP_LIFT_M
    config["main"]["pre_release_hover"] = DEFAULT_PRE_RELEASE_HOVER_M
    config["main"]["pre_release_descend"] = DEFAULT_PRE_RELEASE_DESCEND_M
    config["main"]["post_release_retreat"] = DEFAULT_POST_RELEASE_RETREAT_M
    config["main"]["max_movel_step_m"] = float(config["main"].get("max_movel_step_m", DEFAULT_MAX_MOVEL_STEP_M))
    config["main"]["max_movel_rot_step_deg"] = float(config["main"].get("max_movel_rot_step_deg", DEFAULT_MAX_MOVEL_ROT_STEP_DEG))
    config["main"]["min_movel_step_m"] = float(config["main"].get("min_movel_step_m", DEFAULT_MIN_MOVEL_STEP_M))
    config["main"]["min_movel_rot_step_deg"] = float(config["main"].get("min_movel_rot_step_deg", DEFAULT_MIN_MOVEL_ROT_STEP_DEG))
    # Real Dobot tool frame convention used here:
    # local +Z = tool forward / insertion direction, local +X = tool right.
    config["main"]["tool_forward_local_axis"] = [0.0, 0.0, 1.0]
    config["main"]["tool_right_local_axis"] = [1.0, 0.0, 0.0]
    config["main"]["grasp_retry_offsets"] = [
        [0.0, 0.0, 0.0],
        [0.0, 0.008, 0.0],
        [0.0, -0.008, 0.0],
    ]
    config["subgoal_solver"]["grasp_axis_local"] = [0.0, 0.0, 1.0]
    config["subgoal_solver"]["grasp_preferred_world_dir"] = [0.0, 0.0, -1.0]
    # The real Dobot path does not have a high-rate impedance controller, so keep the
    # optimization lighter than the simulator defaults.
    config["path_solver"]["sampling_maxfun"] = min(int(config["path_solver"].get("sampling_maxfun", 5000)), 1200)
    config["subgoal_solver"]["sampling_maxfun"] = min(int(config["subgoal_solver"].get("sampling_maxfun", 5000)), 1200)
    config["path_solver"]["opt_pos_step_size"] = 0.10
    config["path_solver"]["opt_rot_step_size"] = 0.60
    config["path_solver"]["opt_interpolate_pos_step_size"] = 0.03
    config["path_solver"]["opt_interpolate_rot_step_size"] = 0.18
    # Match the original ReKep real-world proposal stage more closely:
    # DINOv2 + SAM + k-means-per-mask, then de-duplicate candidates within 8 cm.
    config["keypoint_proposer"]["min_dist_bt_keypoints"] = 0.08
    config["keypoint_proposer"]["max_mask_ratio"] = 0.45
    return config



def _depth_to_base_points(depth_image: np.ndarray, camera_calibration: Dict[str, Any]) -> np.ndarray:
    depth = np.asarray(depth_image, dtype=np.float32)
    if depth.ndim == 3:
        depth = depth[..., 0]
    color = (camera_calibration or {}).get("color_intrinsic") or {}
    transform = np.asarray((camera_calibration or {}).get("T_base_camera") or np.eye(4), dtype=np.float64)
    fx = float(color["fx"])
    fy = float(color["fy"])
    cx = float(color["cx"])
    cy = float(color["cy"])
    h, w = depth.shape[:2]
    uu, vv = np.meshgrid(np.arange(w, dtype=np.float32), np.arange(h, dtype=np.float32))
    z = depth.astype(np.float64)
    x = (uu - fx * 0 + uu - uu)  # keep dtype warm; overwritten below
    x = (uu - cx) / fx * z
    y = (vv - cy) / fy * z
    ones = np.ones_like(z)
    cam = np.stack([x, y, z, ones], axis=-1)
    base = cam.reshape(-1, 4) @ transform.T
    return base[:, :3].reshape(h, w, 3)



def _build_sam_cluster_mask(
    rgb_bgr: np.ndarray,
    points_base: np.ndarray,
    depth_image: np.ndarray,
    *,
    bounds_min: np.ndarray,
    bounds_max: np.ndarray,
    min_mask_area_px: int = 200,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    depth = np.asarray(depth_image, dtype=np.float32)
    if depth.ndim == 3:
        depth = depth[..., 0]
    valid_mask = np.isfinite(depth) & (depth > 0.05) & (depth < 3.0)
    if not np.any(valid_mask):
        raise RuntimeError("no valid depth points for candidate proposal")

    bounds_pad_min = np.asarray(bounds_min, dtype=np.float64).copy()
    bounds_pad_max = np.asarray(bounds_max, dtype=np.float64).copy()
    bounds_pad_min[:2] -= 0.05
    bounds_pad_max[:2] += 0.05
    bounds_pad_max[2] += 0.05
    in_workspace = np.all(points_base >= bounds_pad_min[None, None, :], axis=-1) & np.all(points_base <= bounds_pad_max[None, None, :], axis=-1)
    candidate_mask = valid_mask & in_workspace
    candidate_points = points_base[candidate_mask]
    candidate_point_count = int(candidate_points.shape[0])
    if candidate_point_count < 300:
        raise RuntimeError(f"insufficient workspace points for candidate proposal: {candidate_point_count}")

    generator = _get_solver_sam_mask_generator()
    rgb_rgb = cv2.cvtColor(np.asarray(rgb_bgr, dtype=np.uint8), cv2.COLOR_BGR2RGB)
    try:
        raw_masks = generator.generate(rgb_rgb)
    except Exception as exc:
        raise RuntimeError(f"SAM automatic mask generation failed: {exc}") from exc

    labels = np.zeros(depth.shape[:2], dtype=np.int32)
    kept_masks = []
    sorted_masks = sorted(raw_masks, key=lambda item: int(item.get("area", 0) or 0), reverse=True)
    for item in sorted_masks:
        seg = item.get("segmentation")
        if seg is None:
            continue
        seg = np.asarray(seg, dtype=bool)
        if seg.shape[:2] != depth.shape[:2]:
            continue
        mask = seg & candidate_mask
        area = int(np.count_nonzero(mask))
        if area < int(min_mask_area_px):
            continue
        kept_masks.append(
            {
                "mask": mask,
                "area": area,
                "predicted_iou": float(item.get("predicted_iou", 0.0) or 0.0),
                "stability_score": float(item.get("stability_score", 0.0) or 0.0),
            }
        )

    if not kept_masks:
        raise RuntimeError("SAM returned no usable masks inside the calibrated workspace")

    for idx, item in enumerate(kept_masks, start=1):
        labels[item["mask"]] = int(idx)

    debug = {
        "valid_point_count": int(np.count_nonzero(valid_mask)),
        "workspace_point_count": int(np.count_nonzero(candidate_mask)),
        "sam_raw_mask_count": int(len(raw_masks)),
        "sam_kept_mask_count": int(len(kept_masks)),
        "sam_min_mask_area_px": int(min_mask_area_px),
        "mask_areas_px": [int(item["area"]) for item in kept_masks],
        "mask_predicted_iou": [float(item["predicted_iou"]) for item in kept_masks],
        "mask_stability_score": [float(item["stability_score"]) for item in kept_masks],
    }
    return labels, debug



def _make_mask_debug_image(rgb_bgr: np.ndarray, labels: np.ndarray) -> np.ndarray:
    overlay = rgb_bgr.copy()
    unique_labels = [int(v) for v in np.unique(labels) if int(v) > 0]
    if not unique_labels:
        return overlay
    colors = [
        (0, 255, 0),
        (0, 200, 255),
        (255, 180, 0),
        (255, 0, 255),
        (0, 128, 255),
        (255, 0, 0),
    ]
    for idx, label in enumerate(unique_labels):
        mask = labels == int(label)
        color = np.array(colors[idx % len(colors)], dtype=np.uint8)
        overlay[mask] = (0.55 * overlay[mask] + 0.45 * color).astype(np.uint8)
        ys, xs = np.where(mask)
        if len(xs) == 0:
            continue
        cv2.putText(
            overlay,
            f"obj{label}",
            (int(np.median(xs)), int(np.median(ys))),
            cv2.FONT_HERSHEY_SIMPLEX,
            0.6,
            (255, 255, 255),
            2,
        )
    return overlay



def propose_candidate_keypoints(
    *,
    rgb_bgr: np.ndarray,
    depth_image: np.ndarray,
    camera_calibration: Dict[str, Any],
    output_prefix: str,
    output_dir: str | Path,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    bounds_min = np.asarray(config["keypoint_proposer"]["bounds_min"], dtype=np.float64)
    bounds_max = np.asarray(config["keypoint_proposer"]["bounds_max"], dtype=np.float64)
    points_base = _depth_to_base_points(depth_image, camera_calibration)
    masks, mask_debug = _build_sam_cluster_mask(
        rgb_bgr,
        points_base,
        depth_image,
        bounds_min=bounds_min,
        bounds_max=bounds_max,
    )

    proposer = KeypointProposer(config["keypoint_proposer"])
    rgb_rgb = cv2.cvtColor(np.asarray(rgb_bgr, dtype=np.uint8), cv2.COLOR_BGR2RGB)
    candidate_points, projected_rgb, metadata = proposer.get_keypoints(rgb_rgb, points_base, masks, return_metadata=True)
    candidate_pixels = np.asarray(metadata.get("candidate_pixels", []), dtype=np.int32)
    rigid_groups = np.asarray(metadata.get("candidate_rigid_group_ids", []), dtype=np.int32)
    if candidate_points.shape[0] == 0:
        raise RuntimeError("DINOv2 proposer returned zero candidate keypoints")

    overlay_bgr = cv2.cvtColor(np.asarray(projected_rgb, dtype=np.uint8), cv2.COLOR_RGB2BGR)
    overlay_path = output_dir / f"{output_prefix}.keypoints.png"
    mask_path = output_dir / f"{output_prefix}.masks.png"
    cv2.imwrite(str(overlay_path), overlay_bgr)
    cv2.imwrite(str(mask_path), _make_mask_debug_image(rgb_bgr, masks))

    keypoints_2d = {}
    keypoints_3d = {}
    rigid_group_payload = {}
    for idx, point in enumerate(candidate_points):
        key = str(idx)
        pixel = candidate_pixels[idx] if idx < len(candidate_pixels) else np.array([0, 0], dtype=np.int32)
        keypoints_2d[key] = {
            "u": float(pixel[1]),
            "v": float(pixel[0]),
            "label": f"candidate_{idx}",
            "rigid_group_id": int(rigid_groups[idx]) if idx < len(rigid_groups) else -1,
        }
        keypoints_3d[key] = [float(v) for v in point.tolist()]
        rigid_group_payload[key] = int(rigid_groups[idx]) if idx < len(rigid_groups) else -1

    debug_path = output_dir / f"{output_prefix}.proposal_debug.json"
    debug_payload = {
        "proposal_method": "dinov2_sam_mask_kmeans",
        "mask_debug": mask_debug,
        "candidate_count": int(candidate_points.shape[0]),
        "keypoints_2d": keypoints_2d,
        "keypoints_3d": keypoints_3d,
        "rigid_group_ids": rigid_group_payload,
        "mask_overlay_path": str(mask_path),
        "candidate_overlay_path": str(overlay_path),
    }
    debug_path.write_text(json.dumps(_jsonable(debug_payload), ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "visible": True,
        "reason": f"proposed {candidate_points.shape[0]} DINOv2 candidate keypoints from SAM masks",
        "proposal_method": "dinov2_sam_mask_kmeans",
        "raw_output": "",
        "vlm": {},
        "keypoints_2d": keypoints_2d,
        "keypoints_3d": keypoints_3d,
        "rigid_group_ids": rigid_group_payload,
        "schema": [
            {"id": idx, "label": f"candidate_{idx}", "object": f"rigid_group_{rigid_group_payload[str(idx)]}", "purpose": "candidate_keypoint"}
            for idx in range(candidate_points.shape[0])
        ],
        "overlay_path": str(overlay_path),
        "mask_overlay_path": str(mask_path),
        "proposal_debug_path": str(debug_path),
        "proposal_debug": debug_payload,
    }



def _pose_quat_to_movel_action(pose_quat: np.ndarray, *, arm: str) -> Dict[str, Any]:
    pos_mm = np.asarray(pose_quat[:3], dtype=np.float64) * 1000.0
    euler_deg = np.rad2deg(T.quat2euler(np.asarray(pose_quat[3:], dtype=np.float64)))
    return {
        "type": "movel",
        "arm": str(arm),
        "units": "mm_deg",
        "pose": [float(pos_mm[0]), float(pos_mm[1]), float(pos_mm[2]), float(euler_deg[0]), float(euler_deg[1]), float(euler_deg[2])],
    }



def _grasp_cost_closure(active_grasped: set[int]):
    def _cost(keypoint_idx):
        return 0 if int(keypoint_idx) in active_grasped else 1
    return _cost



def _load_stage_constraints(program_dir: str | Path, stage_info: Dict[str, Any], active_grasped: set[int]) -> Tuple[List[Any], List[Any]]:
    subgoal_path = stage_info.get("subgoal_constraints_path")
    path_path = stage_info.get("path_constraints_path")
    grasp_cost_fn = _grasp_cost_closure(active_grasped)
    subgoal_constraints = load_functions_from_txt(str(subgoal_path), grasp_cost_fn) if subgoal_path and Path(subgoal_path).exists() else []
    path_constraints = load_functions_from_txt(str(path_path), grasp_cost_fn) if path_path and Path(path_path).exists() else []
    return subgoal_constraints, path_constraints


def _sanitize_stage_constraints(stage_info: Dict[str, Any], subgoal_constraints: List[Any], path_constraints: List[Any]) -> Tuple[List[Any], List[Any], Dict[str, Any]]:
    debug: Dict[str, Any] = {
        "release_stage_subgoal_constraints_original": int(len(subgoal_constraints)),
        "release_stage_subgoal_constraints_kept": int(len(subgoal_constraints)),
        "release_stage_constraint_sanitized": False,
    }
    is_release_stage = int(stage_info.get("release_keypoint", -1)) >= 0
    if is_release_stage and len(subgoal_constraints) > 1:
        subgoal_constraints = subgoal_constraints[:1]
        debug["release_stage_subgoal_constraints_kept"] = 1
        debug["release_stage_constraint_sanitized"] = True
    return subgoal_constraints, path_constraints, debug



def _quat_geodesic_deg(quat_a: np.ndarray, quat_b: np.ndarray) -> float:
    qa = np.asarray(quat_a, dtype=np.float64).reshape(-1)[:4]
    qb = np.asarray(quat_b, dtype=np.float64).reshape(-1)[:4]
    qa_norm = np.linalg.norm(qa)
    qb_norm = np.linalg.norm(qb)
    if qa_norm < 1e-9 or qb_norm < 1e-9:
        return 0.0
    qa = qa / qa_norm
    qb = qb / qb_norm
    dot = float(np.clip(np.abs(np.dot(qa, qb)), -1.0, 1.0))
    return float(np.rad2deg(2.0 * math.acos(dot)))


def _interpolate_pose_quat(start_pose: np.ndarray, target_pose: np.ndarray, fraction: float) -> np.ndarray:
    start = np.asarray(start_pose, dtype=np.float64)
    target = np.asarray(target_pose, dtype=np.float64)
    frac = float(np.clip(fraction, 0.0, 1.0))
    pos = start[:3] + frac * (target[:3] - start[:3])
    quat = T.quat_slerp(start[3:], target[3:], frac)
    quat = np.asarray(quat, dtype=np.float64).reshape(-1)[:4]
    quat_norm = np.linalg.norm(quat)
    if quat_norm > 1e-9:
        quat = quat / quat_norm
    return np.concatenate([pos, quat], axis=0)


def _split_pose_segment(
    start_pose: np.ndarray,
    target_pose: np.ndarray,
    *,
    max_step_m: float,
    max_rot_step_deg: float,
) -> List[np.ndarray]:
    start = np.asarray(start_pose, dtype=np.float64)
    target = np.asarray(target_pose, dtype=np.float64)
    pos_delta = float(np.linalg.norm(target[:3] - start[:3]))
    rot_delta = _quat_geodesic_deg(target[3:], start[3:])
    pos_parts = 1 if max_step_m <= 1e-9 else int(math.ceil(pos_delta / max(max_step_m, 1e-9)))
    rot_parts = 1 if max_rot_step_deg <= 1e-9 else int(math.ceil(rot_delta / max(max_rot_step_deg, 1e-9)))
    num_parts = max(1, pos_parts, rot_parts)
    return [_interpolate_pose_quat(start, target, step / float(num_parts)) for step in range(1, num_parts + 1)]


def _movel_limits_from_config(config: Dict[str, Any]) -> Dict[str, float]:
    main_cfg = config.get("main", {}) if isinstance(config, dict) else {}
    try:
        max_step_m = float(main_cfg.get("max_movel_step_m", DEFAULT_MAX_MOVEL_STEP_M))
    except Exception:
        max_step_m = float(DEFAULT_MAX_MOVEL_STEP_M)
    try:
        max_rot_step_deg = float(main_cfg.get("max_movel_rot_step_deg", DEFAULT_MAX_MOVEL_ROT_STEP_DEG))
    except Exception:
        max_rot_step_deg = float(DEFAULT_MAX_MOVEL_ROT_STEP_DEG)
    try:
        min_step_m = float(main_cfg.get("min_movel_step_m", DEFAULT_MIN_MOVEL_STEP_M))
    except Exception:
        min_step_m = float(DEFAULT_MIN_MOVEL_STEP_M)
    try:
        min_rot_step_deg = float(main_cfg.get("min_movel_rot_step_deg", DEFAULT_MIN_MOVEL_ROT_STEP_DEG))
    except Exception:
        min_rot_step_deg = float(DEFAULT_MIN_MOVEL_ROT_STEP_DEG)
    return {
        "max_step_m": max(0.005, max_step_m),
        "max_rot_step_deg": max(1.0, max_rot_step_deg),
        "min_step_m": max(0.0, min_step_m),
        "min_rot_step_deg": max(0.0, min_rot_step_deg),
    }


def _control_points_to_actions(
    current_ee_pose: np.ndarray,
    control_points: np.ndarray,
    *,
    arm: str,
    max_step_m: float = DEFAULT_MAX_MOVEL_STEP_M,
    max_rot_step_deg: float = DEFAULT_MAX_MOVEL_ROT_STEP_DEG,
    min_step_m: float = DEFAULT_MIN_MOVEL_STEP_M,
    min_rot_step_deg: float = DEFAULT_MIN_MOVEL_ROT_STEP_DEG,
) -> List[Dict[str, Any]]:
    if control_points is None or len(control_points) == 0:
        return []
    actions: List[Dict[str, Any]] = []
    last_pose = np.asarray(current_ee_pose, dtype=np.float64)
    for target_pose in control_points:
        target_pose = np.asarray(target_pose, dtype=np.float64)
        segment_points = _split_pose_segment(
            last_pose,
            target_pose,
            max_step_m=float(max_step_m),
            max_rot_step_deg=float(max_rot_step_deg),
        )
        for pose in segment_points:
            pos_delta = float(np.linalg.norm(pose[:3] - last_pose[:3]))
            rot_delta = _quat_geodesic_deg(pose[3:], last_pose[3:])
            if pos_delta < float(min_step_m) and rot_delta < float(min_rot_step_deg):
                continue
            actions.append(_pose_quat_to_movel_action(pose, arm=arm))
            last_pose = pose
    return actions


def _control_points_to_actions_for_ctx(
    ctx: RealSolverContext,
    current_ee_pose: np.ndarray,
    control_points: np.ndarray,
    *,
    arm: str,
) -> List[Dict[str, Any]]:
    limits = _movel_limits_from_config(ctx.config)
    return _control_points_to_actions(
        current_ee_pose,
        control_points,
        arm=arm,
        max_step_m=float(limits["max_step_m"]),
        max_rot_step_deg=float(limits["max_rot_step_deg"]),
        min_step_m=float(limits["min_step_m"]),
        min_rot_step_deg=float(limits["min_rot_step_deg"]),
    )



def _execute_action_list(adapter, actions: List[Dict[str, Any]], *, execute_motion: bool, action_interval_s: float, emit_progress, stage: int) -> Tuple[List[Dict[str, Any]], str]:
    records: List[Dict[str, Any]] = []
    execution_error = ""
    for idx, action in enumerate(actions, start=1):
        try:
            result = adapter.execute_action(action, execute_motion=bool(execute_motion))
            records.append({"index": idx, "ok": True, "result": result, "action": action})
            emit_progress(f"[rekep-solver][stage={stage}] action[{idx}] {action.get('type')} ok")
            if bool(execute_motion) and float(action_interval_s) > 0.0 and idx < len(actions):
                emit_progress(f"[rekep-solver][stage={stage}] action[{idx}] cooldown {float(action_interval_s):.1f}s")
                time.sleep(float(action_interval_s))
        except Exception as exc:
            records.append({"index": idx, "ok": False, "error": str(exc), "action": action})
            execution_error = str(exc)
            emit_progress(f"[rekep-solver][stage={stage}] action[{idx}] {action.get('type')} failed: {exc}")
            break
    return records, execution_error



def _pose_with_local_offset(pose_quat: np.ndarray, local_offset: np.ndarray) -> np.ndarray:
    pose_quat = np.asarray(pose_quat, dtype=np.float64)
    local_offset = np.asarray(local_offset, dtype=np.float64)
    pose_out = pose_quat.copy()
    pose_out[:3] = pose_out[:3] + T.quat2mat(pose_quat[3:]) @ local_offset
    return pose_out


def _pose_with_world_offset(pose_quat: np.ndarray, world_offset: np.ndarray) -> np.ndarray:
    pose_quat = np.asarray(pose_quat, dtype=np.float64)
    world_offset = np.asarray(world_offset, dtype=np.float64)
    pose_out = pose_quat.copy()
    pose_out[:3] = pose_out[:3] + world_offset
    return pose_out


def _tool_forward_axis(config: Dict[str, Any]) -> np.ndarray:
    main_cfg = config.get("main", {}) if isinstance(config, dict) else {}
    return _normalized_axis(main_cfg.get("tool_forward_local_axis", [1.0, 0.0, 0.0]), np.array([1.0, 0.0, 0.0], dtype=np.float64))


def _clamp_pose_to_workspace(pose_quat: np.ndarray, ctx: RealSolverContext, *, margin: float = 0.005) -> np.ndarray:
    pose = np.asarray(pose_quat, dtype=np.float64).copy()
    lower = np.asarray(ctx.bounds_min, dtype=np.float64) + float(margin)
    upper = np.asarray(ctx.bounds_max, dtype=np.float64) - float(margin)
    pose[:3] = np.clip(pose[:3], lower, upper)
    return pose


def _update_ctx_pose_from_action(ctx: RealSolverContext, action: Dict[str, Any]) -> None:
    if str(action.get("type", "")).strip().lower() != "movel":
        return
    pose = np.asarray(action.get("pose", []), dtype=np.float64).reshape(-1)
    if pose.size < 6:
        return
    pos_m = pose[:3] / 1000.0
    quat = T.euler2quat(np.deg2rad(pose[3:6]))
    ctx.current_ee_pose = np.concatenate([pos_m, quat], axis=0)
    ctx.refresh_attached_world_keypoints()


def _make_transport_actions(ctx: RealSolverContext, *, target_pose: np.ndarray, arm: str) -> List[Dict[str, Any]]:
    current_pose = np.asarray(ctx.current_ee_pose, dtype=np.float64).copy()
    target_pose = np.asarray(target_pose, dtype=np.float64).copy()
    main_cfg = ctx.config.get("main", {})
    hover = float(main_cfg.get("pre_release_hover", DEFAULT_PRE_RELEASE_HOVER_M))
    descend = float(main_cfg.get("pre_release_descend", DEFAULT_PRE_RELEASE_DESCEND_M))
    safe_top_z = float(ctx.bounds_max[2]) - 0.005
    transport_z = min(safe_top_z, max(current_pose[2], target_pose[2] + hover))
    carry_pose = current_pose.copy()
    carry_pose[2] = transport_z
    hover_pose = target_pose.copy()
    hover_pose[2] = min(safe_top_z, target_pose[2] + hover)
    place_pose = target_pose.copy()
    place_pose[2] = min(hover_pose[2], target_pose[2] + max(descend, 0.0))
    carry_pose = _clamp_pose_to_workspace(carry_pose, ctx)
    hover_pose = _clamp_pose_to_workspace(hover_pose, ctx)
    place_pose = _clamp_pose_to_workspace(place_pose, ctx)
    poses = np.asarray([carry_pose, hover_pose, place_pose], dtype=np.float64)
    return _control_points_to_actions_for_ctx(ctx, current_pose, poses, arm=arm)


def _execute_post_grasp_lift(
    ctx: RealSolverContext,
    *,
    execute_motion: bool,
    action_interval_s: float,
    emit_progress,
    stage: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    lift = float(ctx.config["main"].get("post_grasp_lift", DEFAULT_POST_GRASP_LIFT_M))
    if lift <= 1e-6:
        return [], [], ""
    target_pose = _pose_with_world_offset(np.asarray(ctx.current_ee_pose, dtype=np.float64), np.array([0.0, 0.0, lift], dtype=np.float64))
    target_pose = _clamp_pose_to_workspace(target_pose, ctx)
    actions = _control_points_to_actions_for_ctx(
        ctx,
        np.asarray(ctx.current_ee_pose, dtype=np.float64),
        np.asarray([target_pose], dtype=np.float64),
        arm=ctx.active_arm,
    )
    if not actions:
        return [], [], ""
    records, execution_error = _execute_action_list(
        ctx.adapter,
        actions,
        execute_motion=execute_motion,
        action_interval_s=action_interval_s,
        emit_progress=emit_progress,
        stage=stage,
    )
    if not execution_error:
        _update_ctx_pose_from_action(ctx, actions[-1])
    return actions, records, execution_error



def _execute_grasp_routine(ctx: RealSolverContext, *, grasp_keypoint: int, execute_motion: bool, action_interval_s: float, emit_progress, stage: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    pregrasp_pose = np.asarray(ctx.current_ee_pose, dtype=np.float64).copy()
    retry_offsets = [np.asarray(v, dtype=np.float64) for v in ctx.config["main"].get("grasp_retry_offsets", [[0.0, 0.0, 0.0]])]
    retreat_backoff = float(ctx.config["main"].get("grasp_retry_backoff", 0.012))
    settle_time = float(ctx.config["main"].get("grasp_retry_settle_time", 0.15))
    grasp_depth = float(ctx.config["main"].get("grasp_depth", DEFAULT_REAL_GRASP_DEPTH_M))
    forward_axis = _tool_forward_axis(ctx.config)

    actions: List[Dict[str, Any]] = []
    execution_records: List[Dict[str, Any]] = []
    execution_error = ""
    for attempt_idx, offset in enumerate(retry_offsets, start=1):
        if attempt_idx > 1:
            retry_pose = _pose_with_local_offset(pregrasp_pose, offset - retreat_backoff * forward_axis)
            actions.append({"type": "open_gripper", "arm": ctx.active_arm})
            actions.extend(
                _control_points_to_actions_for_ctx(
                    ctx,
                    np.asarray(ctx.current_ee_pose, dtype=np.float64),
                    np.asarray([retry_pose], dtype=np.float64),
                    arm=ctx.active_arm,
                )
            )
        grasp_pose = _pose_with_local_offset(pregrasp_pose, offset + grasp_depth * forward_axis)
        actions.extend(
            _control_points_to_actions_for_ctx(
                ctx,
                np.asarray(ctx.current_ee_pose, dtype=np.float64),
                np.asarray([grasp_pose], dtype=np.float64),
                arm=ctx.active_arm,
            )
        )
        actions.append({"type": "close_gripper", "arm": ctx.active_arm})
        if settle_time > 0:
            actions.append({"type": "wait", "seconds": settle_time})
        # The live Dobot stack does not expose a reliable object-contact signal yet.
        # Execute the first grasp attempt and stop there; offsets remain available for manual tuning.
        break

    execution_records, execution_error = _execute_action_list(
        ctx.adapter,
        actions,
        execute_motion=execute_motion,
        action_interval_s=action_interval_s,
        emit_progress=emit_progress,
        stage=stage,
    )
    if not execution_error and actions:
        for action in reversed(actions):
            if str(action.get("type", "")).strip().lower() == "movel":
                _update_ctx_pose_from_action(ctx, action)
                break
        ctx.mark_grasped_group(int(grasp_keypoint))
    return actions, execution_records, execution_error



def _execute_release_routine(ctx: RealSolverContext, *, execute_motion: bool, action_interval_s: float, emit_progress, stage: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], str]:
    actions = [
        {"type": "open_gripper", "arm": ctx.active_arm},
        {"type": "wait", "seconds": DEFAULT_RELEASE_OPEN_WAIT_S},
    ]
    records, execution_error = _execute_action_list(
        ctx.adapter,
        actions,
        execute_motion=execute_motion,
        action_interval_s=action_interval_s,
        emit_progress=emit_progress,
        stage=stage,
    )
    if execution_error:
        return actions, records, execution_error

    ctx.clear_grasp()
    retreat = float(ctx.config["main"].get("post_release_retreat", DEFAULT_POST_RELEASE_RETREAT_M))
    if retreat > 1e-6:
        retreat_pose = _pose_with_world_offset(np.asarray(ctx.current_ee_pose, dtype=np.float64), np.array([0.0, 0.0, retreat], dtype=np.float64))
        retreat_pose = _clamp_pose_to_workspace(retreat_pose, ctx)
        retreat_actions = _control_points_to_actions_for_ctx(
            ctx,
            np.asarray(ctx.current_ee_pose, dtype=np.float64),
            np.asarray([retreat_pose], dtype=np.float64),
            arm=ctx.active_arm,
        )
        if retreat_actions:
            retreat_records, execution_error = _execute_action_list(
                ctx.adapter,
                retreat_actions,
                execute_motion=execute_motion,
                action_interval_s=action_interval_s,
                emit_progress=emit_progress,
                stage=stage,
            )
            actions.extend(retreat_actions)
            records.extend(retreat_records)
            if not execution_error:
                _update_ctx_pose_from_action(ctx, retreat_actions[-1])
    return actions, records, execution_error



def execute_solver_program(
    *,
    program: Dict[str, Any],
    planning_keypoint_obs: Dict[str, Any],
    adapter,
    execute_motion: bool,
    action_interval_s: float,
    state_dir: str | Path,
    frame_prefix: str,
    emit_progress,
    arm: str = "right",
    grasp_depth_m: float = DEFAULT_REAL_GRASP_DEPTH_M,
) -> Dict[str, Any]:
    config = build_real_solver_config(arm=arm, grasp_depth_m=grasp_depth_m)
    ctx = RealSolverContext(config=config, adapter=adapter, emit_progress=emit_progress, arm=arm)
    forward_axis = _tool_forward_axis(config)
    ctx.set_initial_scene(
        planning_keypoint_obs.get("keypoints_3d", {}),
        planning_keypoint_obs.get("rigid_group_ids", {}),
        arm=arm,
    )

    state_dir = Path(state_dir)
    frames_dir = state_dir / "frames"
    frames_dir.mkdir(parents=True, exist_ok=True)
    stage_results: List[Dict[str, Any]] = []
    execution_error = None

    for stage_info in program.get("stages", []):
        stage = int(stage_info.get("stage", 0) or 0)
        subgoal_constraints, path_constraints = _load_stage_constraints(program.get("program_dir", ""), stage_info, ctx.grasped_keypoints)
        subgoal_constraints, path_constraints, constraint_debug = _sanitize_stage_constraints(stage_info, subgoal_constraints, path_constraints)
        is_grasp_stage = int(stage_info.get("grasp_keypoint", -1)) >= 0
        is_release_stage = int(stage_info.get("release_keypoint", -1)) >= 0

        full_keypoints = ctx.full_keypoints()
        movable_mask = ctx.movable_mask()
        collision_points = ctx.current_collision_points()
        joint_seed = ctx.current_joint_seed()
        subgoal_pose, subgoal_debug = ctx.subgoal_solver.solve(
            ctx.current_ee_pose,
            full_keypoints,
            movable_mask,
            subgoal_constraints,
            path_constraints,
            ctx.sdf_voxels,
            collision_points,
            is_grasp_stage,
            joint_seed,
            from_scratch=True,
        )
        if is_grasp_stage:
            subgoal_homo = T.pose2mat([subgoal_pose[:3], subgoal_pose[3:]])
            subgoal_pose = np.asarray(subgoal_pose, dtype=np.float64).copy()
            subgoal_pose[:3] += subgoal_homo[:3, :3] @ (-float(grasp_depth_m) / 2.0 * forward_axis)

        path_control_points, path_debug = ctx.path_solver.solve(
            ctx.current_ee_pose,
            subgoal_pose,
            full_keypoints,
            movable_mask,
            path_constraints,
            ctx.sdf_voxels,
            collision_points,
            joint_seed,
            from_scratch=True,
        )
        if is_release_stage:
            move_actions = _make_transport_actions(ctx, target_pose=np.asarray(subgoal_pose, dtype=np.float64), arm=ctx.active_arm)
        else:
            move_actions = _control_points_to_actions_for_ctx(ctx, ctx.current_ee_pose, path_control_points, arm=ctx.active_arm)
        plan_payload = {
            "actions": move_actions,
            "notes": "solver-based SE(3) path from subgoal/path solver",
            "stage_goal_summary": f"solver stage {stage}",
            "solver_debug": {
                "subgoal": _jsonable(subgoal_debug),
                "path": _jsonable(path_debug),
                "constraint_debug": constraint_debug,
            },
        }
        plan_path = frames_dir / f"{frame_prefix}_stage{stage}_attempt1.stage_plan.txt"

        execution_records, stage_error = _execute_action_list(
            adapter,
            move_actions,
            execute_motion=execute_motion,
            action_interval_s=action_interval_s,
            emit_progress=emit_progress,
            stage=stage,
        )
        if not stage_error and move_actions:
            _update_ctx_pose_from_action(ctx, move_actions[-1])

        routine_actions: List[Dict[str, Any]] = []
        routine_records: List[Dict[str, Any]] = []
        if not stage_error and is_grasp_stage:
            routine_actions, routine_records, stage_error = _execute_grasp_routine(
                ctx,
                grasp_keypoint=int(stage_info.get("grasp_keypoint", -1)),
                execute_motion=execute_motion,
                action_interval_s=action_interval_s,
                emit_progress=emit_progress,
                stage=stage,
            )
            if not stage_error:
                lift_actions, lift_records, stage_error = _execute_post_grasp_lift(
                    ctx,
                    execute_motion=execute_motion,
                    action_interval_s=action_interval_s,
                    emit_progress=emit_progress,
                    stage=stage,
                )
                routine_actions.extend(lift_actions)
                routine_records.extend(lift_records)
        if not stage_error and is_release_stage:
            release_actions, release_records, stage_error = _execute_release_routine(
                ctx,
                execute_motion=execute_motion,
                action_interval_s=action_interval_s,
                emit_progress=emit_progress,
                stage=stage,
            )
            routine_actions.extend(release_actions)
            routine_records.extend(release_records)

        full_actions = move_actions + routine_actions
        full_records = execution_records + routine_records
        plan_payload["actions"] = full_actions
        if is_grasp_stage:
            plan_payload["notes"] += "; appended explicit grasp-and-lift routine"
        elif is_release_stage:
            plan_payload["notes"] += "; appended explicit hover-descend-release-retreat routine"
        plan_path.write_text(json.dumps(plan_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if stage_error and execution_error is None:
            execution_error = f"stage {stage} failed: {stage_error}"

        stage_results.append(
            {
                "stage": stage,
                "frame_path": "",
                "depth_path": "",
                "overlay_path": planning_keypoint_obs.get("overlay_path", ""),
                "keypoint_obs": planning_keypoint_obs,
                "object_schema": planning_keypoint_obs.get("schema", []),
                "capture_info": {},
                "instruction": "",
                "stage_constraints": {
                    "subgoal_constraints_path": stage_info.get("subgoal_constraints_path", ""),
                    "path_constraints_path": stage_info.get("path_constraints_path", ""),
                    "grasp_keypoint": int(stage_info.get("grasp_keypoint", -1)),
                    "release_keypoint": int(stage_info.get("release_keypoint", -1)),
                    "solver_debug": {
                        "subgoal": _jsonable(subgoal_debug),
                        "path": _jsonable(path_debug),
                        "constraint_debug": constraint_debug,
                    },
                    "movable_mask": movable_mask.tolist(),
                    "collision_point_count": int(collision_points.shape[0]),
                    "attached_group": None if ctx.attached_group is None else int(ctx.attached_group),
                },
                "grasp_state": {
                    "grasped_keypoints": sorted(int(v) for v in ctx.grasped_keypoints),
                    "attached_group": None if ctx.attached_group is None else int(ctx.attached_group),
                },
                "constraint_eval": {},
                "monitor_result": {},
                "recovery_result": {},
                "plan_actions": full_actions,
                "plan_raw_output_path": str(plan_path),
                "execution_records": full_records,
                "execution_error": stage_error,
            }
        )
        if stage_error:
            break

    return {
        "stage_results": stage_results,
        "execution_error": execution_error,
        "config": _jsonable(config),
        "current_ee_pose": _jsonable(ctx.current_ee_pose),
    }
