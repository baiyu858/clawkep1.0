# Real Calibration Files (Sanitized)

This release copy removes device-specific and environment-specific calibration data.

Before running real robot tasks, replace these template files with your own calibrated data:

- `dobot_settings.ini`
- `realsense_config/camera_intrinsics_depth_scales.json`
- `realsense_config/realsense_calibration_*_lastest.json`

Minimum required fields:

- Camera intrinsics (`fx`, `fy`, `ppx`, `ppy`, distortion coefficients)
- Depth scale
- `T_base_camera` / extrinsic transform (or equivalent rotation + translation)
- Camera serial mapping used by your runtime profile names
