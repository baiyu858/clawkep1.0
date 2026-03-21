---
name: clawkep-vla
description: Natural-language embodied operation and scene QA for ClawKep real robot runtime (Dobot XTrainer + RealSense D455) via OpenClaw. Use when the user asks to control the robot arm, start or stop realtime standby camera stream, ask what is in the current scene, execute grasp/place/manipulation tasks, check run progress, or cancel robot jobs. Trigger on Chinese and English requests about 机械臂/机器人控制, 抓取/放置/操纵, 当前画面是什么, 实时视频流, robot status, scene question answering, and long-horizon manipulation.
---

# ClawKep VLA

Operate the real robot runtime from plain user language. Do not require the user to mention tool names or action enums.

## Runtime Defaults

- Use `rekep_sim` as the execution tool backend.
- Use real runtime actions (`real_*`) for real robot work.
- Do not use ACP sessions and do not route to `codex`; all work stays on OpenClaw main streaming flow.
- Use DMXAPI-backed models configured in runtime (default `gpt-5.4`) for dialogue and VLM planning.
- Default to remote deployment over TailScale: `dobotDriver="xtrainer_zmq"`, `dobotHost="<ROBOT_HOST>"`, `dobotPort=6001`.
- Default camera stream: `cameraSource="realsense_zmq://<CAMERA_HOST>:7001/realsense"` unless the user overrides it.
- Default calibration profile for remote camera: `cameraProfile="global3"`, `cameraSerial="<CAMERA_SERIAL>"`, with files `dobot_settings.ini`, `eval_dobot_v1.py`, and `realsense_config/`.
- If user explicitly asks for local direct control, switch to `dobotDriver="xtrainer_sdk"`, `dobotHost="<DOBOT_LOCAL_HOST>"`, `dobotPort=29999`, `dobotMovePort=30003`, `cameraSource="realsense"`.
- Keep `executeMotion=false` by default.
- Set `executeMotion=true` only when the user explicitly asks for physical movement on hardware.

## Intent Routing

- Route "检查环境/预检/ready/preflight" to `action="real_preflight"`.
- Route "启动待机/打开视频流/start standby/start stream" to `action="real_standby_start"`.
- Route "查看待机状态/status" to `action="real_standby_status"`.
- Route "停止待机/关闭视频流/stop standby" to `action="real_standby_stop"`.
- Route "当前画面有什么/what is in the scene/object question" to `action="real_scene_qa"` with `question`.
- Route "抓取/放置/操纵/执行任务/manipulate/pick/place" to `action="real_execute"` (or background equivalent).
- Route "进度/状态/job status" to `action="real_job_status"`.
- Route "取消任务/stop job/cancel" to `action="real_job_cancel"`.
- Route "长任务/连续任务/30分钟任务/longrun" to `action="real_longrun_start"` with `instruction`.
- Route "暂停/继续/换成放左边/别抓那个，抓旁边那个/跳过当前" to `action="real_longrun_command"` with mapped command + `commandText`.
- Route "长任务进度/longrun status" to `action="real_longrun_status"`.
- Route "停止长任务/终止长任务" to `action="real_longrun_stop"`.

## Execution Workflow

1. Resolve intent from user language; do not ask for action enums.
2. Before `real_scene_qa` or `real_execute`, check standby with `real_standby_status`.
3. If standby is not running, start it with `real_standby_start` first.
4. For high-risk manipulation goals that are physically ambiguous, ask one concise clarification.
5. Otherwise execute directly and report outcome.
6. When longrun is active, treat each new human utterance as potential intervention and enqueue it via `real_longrun_command` instead of launching a new execute job.

## Response Contract

- Always send a normal chat reply after tool execution, including failures.
- Do not ask the user to manually call `rekep_sim`.
- For scene QA, include: answer + snapshot path + log path.
- For execution, include: success/failure + key result fields + log path.
- For background execution, include job id and tell the user to ask for progress naturally.
- Keep responses concise and operator-style.
