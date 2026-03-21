Available local robotics tool:
- `rekep_sim`: bridge into `<PROJECT_ROOT>/ReKep/openclaw_bridge.py`
  - Simulation actions: `preflight`, `run`, `run_background`, `job_status`, `job_cancel`, `scene_qa`
  - Real robot actions: `real_preflight`, `real_standby_start`, `real_standby_status`, `real_standby_stop`, `real_scene_qa`, `real_execute`, `real_execute_background`, `real_job_status`, `real_job_cancel`
  - Long-horizon real actions: `real_longrun_start`, `real_longrun_status`, `real_longrun_command`, `real_longrun_stop`
  - Human intervention commands for `real_longrun_command`: `pause`, `resume`, `replace_current`, `replace_plan`, `append_subtask`, `skip_current`, `stop`
  - OpenClaw-triggered runs default to headless mode unless overridden
  - Real runtime supports drivers: `xtrainer_sdk` (local SDK), `xtrainer_zmq` (remote ZMQ robot server), `dashboard_tcp`, `mock`
  - Real runtime supports camera source forms: `realsense` / `realsense:<serial>` / `realsense_zmq://<host>:<port>/<topic>`
  - User should not need to call this tool explicitly; natural-language robotics requests should route via workspace skill `$clawkep-vla`
  - Chat realtime updates should use the fixed 4-line template (阶段/进度百分比/下一步/预计剩余时间), not raw log tail text

Environment notes:
- OpenClaw state is isolated under `<PROJECT_ROOT>/openclaw-runtime/state`
- OpenClaw workspace is `<PROJECT_ROOT>/openclaw-runtime/workspace`
- ReKep Python environment is expected to be `conda` env `rekep`
- XTrainer SDK unpacked at `<PROJECT_ROOT>/.downloads/third_party/dobot_xtrainer`
