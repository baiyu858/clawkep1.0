from __future__ import annotations

import math
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np


def _parse_floats(text: Optional[str], n: int, default: float = 0.0) -> np.ndarray:
    if text is None or text.strip() == "":
        return np.full((n,), default, dtype=float)
    parts = text.replace(",", " ").split()
    if len(parts) != n:
        raise ValueError(f"Expected {n} floats, got {len(parts)} from: {text!r}")
    return np.array([float(p) for p in parts], dtype=float)


def _rpy_to_rot(rpy: np.ndarray) -> np.ndarray:
    roll, pitch, yaw = (float(rpy[0]), float(rpy[1]), float(rpy[2]))
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


def _axis_angle_to_rot(axis: np.ndarray, angle: float) -> np.ndarray:
    axis = np.asarray(axis, dtype=float).reshape(3)
    n = float(np.linalg.norm(axis))
    if n < 1e-12:
        axis = np.array([1.0, 0.0, 0.0], dtype=float)
    else:
        axis = axis / n
    x, y, z = float(axis[0]), float(axis[1]), float(axis[2])
    c = math.cos(angle)
    s = math.sin(angle)
    C = 1.0 - c
    return np.array(
        [
            [x * x * C + c, x * y * C - z * s, x * z * C + y * s],
            [y * x * C + z * s, y * y * C + c, y * z * C - x * s],
            [z * x * C - y * s, z * y * C + x * s, z * z * C + c],
        ],
        dtype=float,
    )


def _make_transform(R: np.ndarray, t: np.ndarray) -> np.ndarray:
    T = np.eye(4, dtype=float)
    T[:3, :3] = R
    T[:3, 3] = t
    return T


def _origin_transform(xyz: np.ndarray, rpy: np.ndarray) -> np.ndarray:
    return _make_transform(_rpy_to_rot(rpy), xyz)


def rot_to_rpy_xyz(R: np.ndarray) -> np.ndarray:
    R = np.asarray(R, dtype=float).reshape(3, 3)
    pitch = math.atan2(-float(R[2, 0]), math.sqrt(float(R[2, 1]) ** 2 + float(R[2, 2]) ** 2))
    if abs(abs(pitch) - (math.pi / 2.0)) < 1e-10:
        roll = 0.0
        yaw = math.atan2(-float(R[0, 1]), float(R[1, 1]))
    else:
        roll = math.atan2(float(R[2, 1]), float(R[2, 2]))
        yaw = math.atan2(float(R[1, 0]), float(R[0, 0]))
    return np.array([roll, pitch, yaw], dtype=float)


@dataclass(frozen=True)
class Joint:
    name: str
    joint_type: str
    parent: str
    child: str
    origin_xyz: np.ndarray
    origin_rpy: np.ndarray
    axis: np.ndarray


def load_urdf(urdf_path: str) -> Tuple[Dict[str, Joint], List[str]]:
    tree = ET.parse(urdf_path)
    root = tree.getroot()
    if root.tag != "robot":
        raise ValueError(f"Not a URDF robot file: {urdf_path}")

    links: List[str] = []
    joints: Dict[str, Joint] = {}

    for link in root.findall("link"):
        name = link.attrib.get("name")
        if name:
            links.append(name)

    for joint_el in root.findall("joint"):
        name = joint_el.attrib["name"]
        joint_type = joint_el.attrib.get("type", "fixed")

        parent_el = joint_el.find("parent")
        child_el = joint_el.find("child")
        if parent_el is None or child_el is None:
            raise ValueError(f"Joint missing parent/child: {name}")
        parent_link = parent_el.attrib["link"]
        child_link = child_el.attrib["link"]

        origin_el = joint_el.find("origin")
        if origin_el is None:
            xyz = np.zeros(3, dtype=float)
            rpy = np.zeros(3, dtype=float)
        else:
            xyz = _parse_floats(origin_el.attrib.get("xyz"), 3, default=0.0)
            rpy = _parse_floats(origin_el.attrib.get("rpy"), 3, default=0.0)

        axis_el = joint_el.find("axis")
        axis = _parse_floats(axis_el.attrib.get("xyz") if axis_el is not None else None, 3, default=0.0)
        if float(np.linalg.norm(axis)) < 1e-12:
            axis = np.array([1.0, 0.0, 0.0], dtype=float)

        joints[name] = Joint(
            name=name,
            joint_type=joint_type,
            parent=parent_link,
            child=child_link,
            origin_xyz=xyz,
            origin_rpy=rpy,
            axis=axis,
        )
    return joints, links


def infer_root_link(links: Sequence[str], joints: Dict[str, Joint]) -> str:
    child_links = {j.child for j in joints.values()}
    roots = [l for l in links if l not in child_links]
    if not roots:
        raise ValueError("Could not infer root link (cycle?)")
    if "base_link" in roots:
        return "base_link"
    if "base_link" in links:
        return "base_link"
    return roots[0]


def infer_end_link(links: Sequence[str]) -> str:
    # Best-effort default for nova2 URDFs.
    for cand in ("Link6", "tool0", "ee_link"):
        if cand in links:
            return cand
    return links[-1]


def chain_joints_to_link(joints: Dict[str, Joint], root_link: str, end_link: str) -> List[Joint]:
    child_to_joint: Dict[str, Joint] = {j.child: j for j in joints.values()}

    path: List[Joint] = []
    cur = end_link
    while cur != root_link:
        j = child_to_joint.get(cur)
        if j is None:
            raise ValueError(f"No path from root={root_link!r} to end={end_link!r} (stuck at link {cur!r})")
        path.append(j)
        cur = j.parent
    path.reverse()
    return path


def forward_kinematics(
    joints: Dict[str, Joint],
    *,
    root_link: str,
    end_link: str,
    q_by_name: Dict[str, float],
    tool_T: Optional[np.ndarray] = None,
) -> np.ndarray:
    chain = chain_joints_to_link(joints, root_link=root_link, end_link=end_link)
    T = np.eye(4, dtype=float)
    for j in chain:
        T = T @ _origin_transform(j.origin_xyz, j.origin_rpy)
        if j.joint_type in ("fixed",):
            continue
        q = float(q_by_name.get(j.name, 0.0))
        if j.joint_type in ("revolute", "continuous"):
            R = _axis_angle_to_rot(j.axis, q)
            T = T @ _make_transform(R, np.zeros(3, dtype=float))
        elif j.joint_type in ("prismatic",):
            axis = np.asarray(j.axis, dtype=float).reshape(3)
            n = float(np.linalg.norm(axis))
            if n > 0:
                axis = axis / n
            T = T @ _make_transform(np.eye(3, dtype=float), axis * q)
        else:
            raise ValueError(f"Unsupported joint type: {j.joint_type!r} for joint {j.name!r}")
    if tool_T is not None:
        T = T @ tool_T
    return T

