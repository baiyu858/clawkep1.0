import { spawn } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_TIMEOUT_MS = 1_800_000;
const DEFAULT_MAX_STDOUT_BYTES = 512_000;
const DEFAULT_STDERR_TAIL_CHARS = 64_000;
const DEFAULT_LIVE_OUTPUT_CHARS = 24_000;
const LIVE_UPDATE_THROTTLE_MS = 250;
const BACKGROUND_MONITOR_MAX_MS = 15_000;
const BACKGROUND_MONITOR_POLL_MS = 1_000;
const REAL_ACTIONS = new Set([
  "real_preflight",
  "real_standby_start",
  "real_standby_status",
  "real_standby_stop",
  "real_scene_qa",
  "real_execute",
  "real_execute_background",
  "real_job_status",
  "real_job_cancel",
  "real_longrun_start",
  "real_longrun_status",
  "real_longrun_command",
  "real_longrun_stop",
]);
const DEFAULT_REMOTE_REALSENSE_ZMQ_PORT = 7001;
const DEFAULT_REMOTE_REALSENSE_TOPIC = "realsense";

function asObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function readString(value, fallback = "") {
  return typeof value === "string" && value.trim() ? value.trim() : fallback;
}

function readBoolean(value, fallback = false) {
  return typeof value === "boolean" ? value : fallback;
}

function readNumber(value, fallback) {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function isRealAction(action) {
  return REAL_ACTIONS.has(readString(action));
}

function isPlainRealsenseSource(value) {
  const source = readString(value).toLowerCase();
  if (!source) {
    return false;
  }
  if (
    source.includes("realsense_zmq://") ||
    source.includes("rs_zmq://") ||
    source.includes("zmq+realsense://")
  ) {
    return false;
  }
  return (
    source === "realsense" ||
    source === "rs" ||
    source === "d455" ||
    source.startsWith("realsense:") ||
    source.startsWith("rs:")
  );
}

function buildRemoteRealsenseSource(host) {
  const resolvedHost = readString(host, "127.0.0.1");
  return `realsense_zmq://${resolvedHost}:${DEFAULT_REMOTE_REALSENSE_ZMQ_PORT}/${DEFAULT_REMOTE_REALSENSE_TOPIC}`;
}

const ACTION_PROGRESS_LABELS = {
  preflight: "ReKep preflight",
  list_tasks: "ReKep task listing",
  run: "ReKep simulation run",
  run_background: "ReKep simulation background run",
  job_status: "ReKep simulation job status",
  job_cancel: "ReKep simulation job cancel",
  scene_qa: "ReKep simulation scene QA",
  real_preflight: "ReKep real preflight",
  real_standby_start: "ReKep real standby start",
  real_standby_status: "ReKep real standby status",
  real_standby_stop: "ReKep real standby stop",
  real_scene_qa: "ReKep real scene QA",
  real_execute: "ReKep real execute",
  real_execute_background: "ReKep real background execute",
  real_job_status: "ReKep real job status",
  real_job_cancel: "ReKep real job cancel",
  real_longrun_start: "ReKep real longrun start",
  real_longrun_status: "ReKep real longrun status",
  real_longrun_command: "ReKep real longrun command",
  real_longrun_stop: "ReKep real longrun stop",
};

function truncateInline(value, maxChars = 180) {
  const normalized = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!normalized) {
    return "";
  }
  if (normalized.length <= maxChars) {
    return normalized;
  }
  return `${normalized.slice(0, Math.max(0, maxChars - 3))}...`;
}

function actionProgressLabel(action) {
  const key = readString(action);
  return ACTION_PROGRESS_LABELS[key] ?? `ReKep ${key || "task"}`;
}

function defaultNextStep(action) {
  const key = readString(action);
  if (key === "scene_qa" || key === "real_scene_qa") {
    return "Capture frame and run VLM QA.";
  }
  if (key === "run" || key === "run_background") {
    return "Load scene and execute plan.";
  }
  if (key === "real_execute" || key === "real_execute_background") {
    return "Capture RGB-D frame, localize keypoints, generate stage constraints, then execute stage plans.";
  }
  if (key === "real_longrun_start") {
    return "Start long-horizon scheduler and first subtask.";
  }
  if (key === "real_longrun_status") {
    return "Read long-horizon job state.";
  }
  if (key === "real_longrun_command") {
    return "Queue human intervention command.";
  }
  if (key === "real_longrun_stop") {
    return "Stop long-horizon job.";
  }
  if (key === "real_standby_start") {
    return "Start standby stream worker.";
  }
  if (key === "real_standby_status") {
    return "Read current standby worker state.";
  }
  if (key === "real_standby_stop") {
    return "Stop standby stream worker.";
  }
  if (key === "preflight" || key === "real_preflight") {
    return "Check runtime blockers.";
  }
  return "Wait for bridge progress events.";
}

function createProgressState(action) {
  const now = Date.now();
  return {
    action: readString(action, "task"),
    stage: "Initializing",
    status: "Starting bridge process.",
    nextStep: defaultNextStep(action),
    startedAtMs: now,
    updatedAtMs: now,
    runLogPath: "",
    tracePath: "",
    planRawPath: "",
    plannedActions: null,
    completedActions: 0,
    failedActions: 0,
    lastEvent: "",
  };
}

function updateProgressFromLine(progress, rawLine) {
  const line = truncateInline(rawLine, 320);
  if (!line) {
    return;
  }
  progress.updatedAtMs = Date.now();
  progress.lastEvent = line;

  let match = line.match(/\[bridge\]\s+run_log_path:\s*(\S+)/);
  if (match?.[1]) {
    progress.runLogPath = match[1];
    progress.stage = "Initialized";
    progress.status = "Run log path resolved.";
    progress.nextStep = "Launch worker process.";
    return;
  }

  if (line.startsWith("[bridge] launch:")) {
    progress.stage = "Launching";
    progress.status = "Bridge worker process started.";
    progress.nextStep = "Prepare runtime environment.";
    return;
  }

  if (line.startsWith("[bridge] runtime:")) {
    progress.stage = "Runtime setup";
    progress.status = truncateInline(line.replace(/^\[bridge\]\s+runtime:\s*/, ""), 140);
    progress.nextStep =
      progress.action === "scene_qa" || progress.action === "real_scene_qa"
        ? "Capture current frame."
        : "Wait for planner execution.";
    return;
  }

  if (line.includes("[real] standby worker started")) {
    progress.stage = "Standby stream";
    progress.status = "Camera standby worker is running.";
    progress.nextStep = "Wait for QA or execute request.";
    return;
  }

  if (line.includes("[real] standby worker stopped")) {
    progress.stage = "Standby stream";
    progress.status = "Camera standby worker stopped.";
    progress.nextStep = "Task complete.";
    return;
  }

  if (line.includes("[real] scene_qa frame=")) {
    progress.stage = "Frame capture";
    progress.status = "Current scene frame captured.";
    progress.nextStep = "Run VLM scene QA.";
    return;
  }

  if (line.includes("[real] execute instruction=")) {
    progress.stage = "Frame capture";
    progress.status = "Instruction accepted and frame captured.";
    progress.nextStep = "Run keypoint localization and stage-constraint generation.";
    return;
  }

  if (line.includes("[longrun] worker started")) {
    progress.stage = "Longrun scheduler";
    progress.status = "Longrun worker process is running.";
    progress.nextStep = "Pick and execute the current subtask.";
    return;
  }

  match = line.match(/\[longrun\]\s+subtask started:\s*(.+)$/i);
  if (match?.[1]) {
    progress.stage = "Subtask planning";
    progress.status = `Current subtask: ${truncateInline(match[1], 120)}`;
    progress.nextStep = "Run subtask execution.";
    return;
  }

  if (line.includes("[longrun] execute job started:")) {
    progress.stage = "Subtask executing";
    progress.status = "Robot action job started.";
    progress.nextStep = "Monitor deviation and completion.";
    return;
  }

  match = line.match(/\[longrun\]\s+monitor status=([a-z_]+)(?:\s+reason=(.*))?/i);
  if (match?.[1]) {
    const monitor = readString(match[1]).toLowerCase();
    const reason = truncateInline(match?.[2] ?? "", 120);
    progress.stage = "Online monitoring";
    progress.status = reason ? `Monitor: ${monitor} (${reason})` : `Monitor: ${monitor}`;
    if (["dropped", "grasp_failed", "target_lost", "blocked", "deviation"].includes(monitor)) {
      progress.nextStep = "Apply recovery strategy and retry.";
    } else if (monitor === "goal_done") {
      progress.nextStep = "Finalize current subtask.";
    } else {
      progress.nextStep = "Continue monitoring loop.";
    }
    return;
  }

  if (line.includes("[longrun] paused by human command")) {
    progress.stage = "Paused";
    progress.status = "Paused by human intervention.";
    progress.nextStep = "Wait for resume/replace command.";
    return;
  }

  if (line.includes("[longrun] resumed by human command")) {
    progress.stage = "Subtask executing";
    progress.status = "Resumed by human intervention.";
    progress.nextStep = "Continue executing current plan.";
    return;
  }

  match = line.match(/\[longrun\]\s+worker finished status=([a-z_]+)/i);
  if (match?.[1]) {
    const finalStatus = readString(match[1]).toLowerCase();
    if (finalStatus === "succeeded") {
      progress.stage = "Completed";
      progress.status = "Longrun job completed.";
      progress.nextStep = "Return final summary.";
    } else {
      progress.stage = "Failed";
      progress.status = `Longrun job finished with status ${finalStatus}.`;
      progress.nextStep = "Return failure summary.";
    }
    return;
  }

  if (line.includes("[vlm] assistant_response_begin")) {
    progress.stage = "VLM reasoning";
    progress.status = "Generating scene answer.";
    progress.nextStep = "Wait for model response.";
    return;
  }

  if (line.includes("[vlm] assistant_response_end")) {
    progress.stage = "VLM reasoning done";
    progress.status = "Model response generated.";
    progress.nextStep = "Package final result.";
    return;
  }

  if (line.includes("[vlm] planner_raw_begin")) {
    progress.stage = "VLM planning";
    progress.status = "Generating action sequence.";
    progress.nextStep = "Wait for planning completion.";
    return;
  }

  if (line.includes("[vlm] planner_raw_end")) {
    progress.stage = "VLM planning done";
    progress.status = "Action sequence generated.";
    progress.nextStep = "Execute robot actions.";
    return;
  }

  match = line.match(/\[vlm\]\s+trace_path:\s*(\S+)/);
  if (match?.[1]) {
    progress.tracePath = match[1];
    progress.status = "VLM trace file generated.";
    progress.nextStep = "Package final result.";
    return;
  }

  match = line.match(/\[vlm\]\s+plan_raw_path:\s*(\S+)/);
  if (match?.[1]) {
    progress.planRawPath = match[1];
    progress.status = "Planner raw output file generated.";
    progress.nextStep = "Execute planned actions.";
    return;
  }

  match = line.match(/\[real\]\s+planned_actions(?:=|:\s*)(\d+)/i);
  if (match?.[1]) {
    progress.stage = "Executing actions";
    progress.plannedActions = Number.parseInt(match[1], 10) || 0;
    progress.completedActions = 0;
    progress.status = `Planned ${progress.plannedActions} robot actions.`;
    progress.nextStep =
      progress.plannedActions > 0 ? `Execute action 1/${progress.plannedActions}.` : "Execute robot actions.";
    return;
  }

  match = line.match(/\[real\]\s+action\[(\d+)\]\s+([^\s]+)\s+(ok|failed)/i);
  if (match?.[1] && match?.[3]) {
    progress.stage = "Executing actions";
    const currentIndex = (Number.parseInt(match[1], 10) || 0) + 1;
    progress.completedActions = Math.max(progress.completedActions, currentIndex);
    if (match[3].toLowerCase() === "failed") {
      progress.failedActions += 1;
      progress.status = `Action ${currentIndex} failed (${match[2]}).`;
      progress.nextStep = "Stop and return failure.";
    } else {
      const total =
        typeof progress.plannedActions === "number" && progress.plannedActions > 0
          ? progress.plannedActions
          : progress.completedActions;
      progress.status = `Action ${currentIndex} completed (${match[2]}).`;
      if (total > progress.completedActions) {
        progress.nextStep = `Execute action ${progress.completedActions + 1}/${total}.`;
      } else {
        progress.nextStep = "Finalize execution result.";
      }
    }
    return;
  }

  if (/\b(error|failed|exception|traceback)\b/i.test(line)) {
    progress.stage = "Failure";
    progress.status = truncateInline(line, 180);
    progress.nextStep = "Return error to user.";
    return;
  }
}

function applyProgressFromText(progress, text) {
  const raw = String(text ?? "");
  if (!raw) {
    return progress;
  }
  const lines = raw.split(/\r?\n/);
  for (const line of lines) {
    updateProgressFromLine(progress, line);
  }
  return progress;
}

function parseIsoTimestampMs(value) {
  const text = readString(value);
  if (!text) {
    return null;
  }
  const ms = Date.parse(text);
  return Number.isFinite(ms) ? ms : null;
}

function clampPercent(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) {
    return 0;
  }
  return Math.max(0, Math.min(100, num));
}

function stagePercentEstimate(progress) {
  const stage = readString(progress.stage).toLowerCase();
  const map = {
    initializing: 4,
    initialized: 10,
    launching: 16,
    "runtime setup": 24,
    "standby stream": 35,
    "frame capture": 42,
    "vlm reasoning": 62,
    "vlm reasoning done": 78,
    "vlm planning": 62,
    "vlm planning done": 78,
    "executing actions": 82,
    "longrun scheduler": 22,
    "subtask planning": 42,
    "subtask executing": 68,
    "online monitoring": 74,
    paused: 58,
    completed: 100,
    failure: 100,
    failed: 100,
  };
  return map[stage] ?? 20;
}

function estimateProgressPercent(progress) {
  const stage = readString(progress.stage).toLowerCase();
  if (stage === "completed") {
    return 100;
  }
  if (stage === "failure" || stage === "failed") {
    return clampPercent(stagePercentEstimate(progress));
  }
  const planned =
    typeof progress.plannedActions === "number" && Number.isFinite(progress.plannedActions)
      ? Math.max(0, progress.plannedActions)
      : 0;
  const completed =
    typeof progress.completedActions === "number" && Number.isFinite(progress.completedActions)
      ? Math.max(0, progress.completedActions)
      : 0;
  if (planned > 0) {
    const ratio = Math.max(0, Math.min(1, completed / planned));
    const percent = 80 + Math.round(ratio * 18);
    return clampPercent(percent);
  }
  return clampPercent(stagePercentEstimate(progress));
}

function formatDurationSeconds(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "约 0 秒";
  }
  const s = Math.round(seconds);
  if (s < 60) {
    return `约 ${s} 秒`;
  }
  const m = Math.floor(s / 60);
  const rem = s % 60;
  if (m < 60) {
    if (rem === 0) {
      return `约 ${m} 分钟`;
    }
    return `约 ${m} 分 ${rem} 秒`;
  }
  const h = Math.floor(m / 60);
  const remM = m % 60;
  if (remM === 0) {
    return `约 ${h} 小时`;
  }
  return `约 ${h} 小时 ${remM} 分钟`;
}

function estimateEtaText(progress, percent, nowMs = Date.now()) {
  const stage = readString(progress.stage).toLowerCase();
  if (stage === "completed") {
    return "已完成";
  }
  if (stage === "failure" || stage === "failed") {
    return "未知（任务失败）";
  }
  const startedAtMs =
    (typeof progress.startedAtMs === "number" && Number.isFinite(progress.startedAtMs) ? progress.startedAtMs : null) ??
    nowMs;
  const elapsedSec = Math.max(0, (nowMs - startedAtMs) / 1000);
  if (percent <= 0) {
    return "计算中";
  }
  if (percent >= 99) {
    return "约 5 秒内";
  }
  if (elapsedSec < 2) {
    return "计算中";
  }
  const totalSec = elapsedSec / (percent / 100);
  const remainSec = Math.max(0, Math.min(7200, totalSec - elapsedSec));
  return formatDurationSeconds(remainSec);
}

function buildProgressUpdateText(progress) {
  const nowMs = Date.now();
  const percent = estimateProgressPercent(progress);
  const eta = estimateEtaText(progress, percent, nowMs);
  const stage = readString(progress.stage, "Running");
  const status = readString(progress.status, "In progress");
  const next = readString(progress.nextStep, "等待下一步事件");
  return [
    `阶段: ${stage} | 状态: ${status}`,
    `进度百分比: ${percent}%`,
    `下一步: ${next}`,
    `预计剩余时间: ${eta}`,
  ].join("\n");
}

function validatePluginConfig(value) {
  const cfg = asObject(value);
  const errors = [];
  const stringKeys = [
    "repoDir",
    "condaExe",
    "condaEnv",
    "pythonBin",
    "bridgeScript",
    "jobStateDir",
    "defaultTask",
    "vlmApiKeyEnv",
    "vlmBaseUrl",
    "vlmModel",
    "defaultRealCameraSource",
    "defaultRealCameraProfile",
    "defaultRealCameraSerial",
    "defaultRealDobotDriver",
    "defaultRealDobotHost",
    "defaultRealDobotSettingsIni",
    "defaultRealCameraExtrinsicScript",
    "defaultRealRealsenseCalibDir",
    "defaultRealModel",
    "defaultRealRekepExecutionMode",
    "defaultRealXtrainerSdkDir",
    "defaultRealFeishuWebhook",
  ];
  for (const key of stringKeys) {
    const raw = cfg[key];
    if (raw !== undefined && typeof raw !== "string") {
      errors.push(`${key} must be a string`);
    }
  }
  for (const key of [
    "defaultUseCachedQuery",
    "defaultHeadless",
    "defaultRealExecuteMotion",
    "defaultRealUseStandbyFrame",
  ]) {
    const raw = cfg[key];
    if (raw !== undefined && typeof raw !== "boolean") {
      errors.push(`${key} must be a boolean`);
    }
  }
  for (const key of [
    "defaultTimeoutMs",
    "defaultHoldUiSeconds",
    "defaultSceneQaCameraId",
    "defaultRealDobotPort",
    "defaultRealDobotMovePort",
    "defaultRealIntervalS",
    "defaultRealCameraTimeoutS",
    "defaultRealStandbyStaleTimeoutS",
    "defaultRealCameraWarmupFrames",
    "defaultRealTemperature",
    "defaultRealMaxTokens",
    "defaultRealMeanshiftJobs",
    "defaultRealLongrunMaxRuntimeMinutes",
    "defaultRealLongrunMonitorIntervalS",
    "defaultRealLongrunRetryLimit",
  ]) {
    const raw = cfg[key];
    if (raw !== undefined && typeof raw !== "number") {
      errors.push(`${key} must be a number`);
    }
  }
  return errors.length > 0 ? { ok: false, errors } : { ok: true, value: cfg };
}

function normalizeConfig(value) {
  const cfg = asObject(value);
  const repoDir = readString(cfg.repoDir);
  const bridgeScript = readString(cfg.bridgeScript, "openclaw_bridge.py");
  return {
    repoDir: repoDir ? path.resolve(repoDir) : "",
    condaExe: readString(cfg.condaExe, "conda"),
    condaEnv: readString(cfg.condaEnv, "rekep"),
    pythonBin: readString(cfg.pythonBin),
    bridgeScript,
    bridgeScriptPath: repoDir ? path.resolve(repoDir, bridgeScript) : bridgeScript,
    jobStateDir: readString(cfg.jobStateDir),
    defaultTask: readString(cfg.defaultTask, "pen"),
    defaultUseCachedQuery: readBoolean(cfg.defaultUseCachedQuery, true),
    defaultHeadless: readBoolean(cfg.defaultHeadless, true),
    defaultHoldUiSeconds: readNumber(cfg.defaultHoldUiSeconds, 0),
    defaultTimeoutMs: readNumber(cfg.defaultTimeoutMs, DEFAULT_TIMEOUT_MS),
    defaultSceneQaCameraId: readNumber(cfg.defaultSceneQaCameraId, 0),
    vlmApiKeyEnv: readString(cfg.vlmApiKeyEnv, "DMXAPI_API_KEY"),
    vlmBaseUrl: readString(cfg.vlmBaseUrl),
    vlmModel: readString(cfg.vlmModel, "gpt-5.4"),
    defaultRealCameraSource: readString(cfg.defaultRealCameraSource, "realsense"),
    defaultRealCameraProfile: readString(cfg.defaultRealCameraProfile, "global3"),
    defaultRealCameraSerial: readString(cfg.defaultRealCameraSerial, ""),
    defaultRealDobotDriver: readString(cfg.defaultRealDobotDriver, "xtrainer_sdk"),
    defaultRealDobotHost: readString(cfg.defaultRealDobotHost, "127.0.0.1"),
    defaultRealDobotSettingsIni: readString(
      cfg.defaultRealDobotSettingsIni,
      repoDir ? path.resolve(repoDir, "real_calibration/dobot_settings.ini") : "",
    ),
    defaultRealCameraExtrinsicScript: readString(
      cfg.defaultRealCameraExtrinsicScript,
      repoDir ? path.resolve(repoDir, "real_calibration/eval_dobot_v1.py") : "",
    ),
    defaultRealRealsenseCalibDir: readString(
      cfg.defaultRealRealsenseCalibDir,
      repoDir ? path.resolve(repoDir, "real_calibration/realsense_config") : "",
    ),
    defaultRealDobotPort: readNumber(cfg.defaultRealDobotPort, 29999),
    defaultRealDobotMovePort: readNumber(cfg.defaultRealDobotMovePort, 30003),
    defaultRealIntervalS: readNumber(cfg.defaultRealIntervalS, 0.2),
    defaultRealExecuteMotion: readBoolean(cfg.defaultRealExecuteMotion, false),
    defaultRealUseStandbyFrame: readBoolean(cfg.defaultRealUseStandbyFrame, false),
    defaultRealCameraTimeoutS: readNumber(cfg.defaultRealCameraTimeoutS, 8),
    defaultRealStandbyStaleTimeoutS: readNumber(cfg.defaultRealStandbyStaleTimeoutS, 15),
    defaultRealCameraWarmupFrames: readNumber(cfg.defaultRealCameraWarmupFrames, 6),
    defaultRealModel: readString(cfg.defaultRealModel, "gpt-5.4"),
    defaultRealRekepExecutionMode:
      readString(cfg.defaultRealRekepExecutionMode, "solver").toLowerCase() === "vlm_stage"
        ? "vlm_stage"
        : "solver",
    defaultRealTemperature: readNumber(cfg.defaultRealTemperature, 0),
    defaultRealMaxTokens: readNumber(cfg.defaultRealMaxTokens, 1024),
    defaultRealMeanshiftJobs: readNumber(cfg.defaultRealMeanshiftJobs, 1),
    defaultRealLongrunMaxRuntimeMinutes: readNumber(cfg.defaultRealLongrunMaxRuntimeMinutes, 30),
    defaultRealLongrunMonitorIntervalS: readNumber(cfg.defaultRealLongrunMonitorIntervalS, 2),
    defaultRealLongrunRetryLimit: readNumber(cfg.defaultRealLongrunRetryLimit, 2),
    defaultRealFeishuWebhook: readString(
      cfg.defaultRealFeishuWebhook,
      readString(process.env.REKEP_FEISHU_WEBHOOK, readString(process.env.FEISHU_WEBHOOK)),
    ),
    defaultRealXtrainerSdkDir: readString(
      cfg.defaultRealXtrainerSdkDir,
      repoDir ? path.resolve(repoDir, "../.downloads/third_party/dobot_xtrainer") : "",
    ),
  };
}

function resolvePluginJobStateDir(api, cfg) {
  const configured = readString(cfg.jobStateDir);
  if (configured) {
    return path.resolve(configured);
  }
  const stateDir = api.runtime?.state?.resolveStateDir?.();
  if (readString(stateDir)) {
    return path.join(stateDir, "rekep");
  }
  return path.join(cfg.repoDir, ".openclaw", "rekep");
}

function buildBridgeEnv(cfg, extras = {}) {
  const env = { ...process.env };
  if (cfg.vlmApiKeyEnv) {
    env.REKEP_VLM_API_KEY_ENV = cfg.vlmApiKeyEnv;
  }
  if (cfg.vlmBaseUrl) {
    env.REKEP_VLM_BASE_URL = cfg.vlmBaseUrl;
  }
  if (cfg.vlmModel) {
    env.REKEP_VLM_MODEL = cfg.vlmModel;
  }
  if (readString(extras.jobStateDir)) {
    env.REKEP_JOB_STATE_DIR = readString(extras.jobStateDir);
  }
  const modeRaw = readString(extras.rekepExecutionMode, cfg.defaultRealRekepExecutionMode).toLowerCase();
  const mode = modeRaw === "vlm_stage" ? "vlm_stage" : "solver";
  env.REKEP_EXECUTION_MODE = mode;
  const meanshiftJobs = Number.isFinite(extras.meanshiftJobs) ? Math.trunc(extras.meanshiftJobs) : Math.trunc(cfg.defaultRealMeanshiftJobs);
  if (Number.isFinite(meanshiftJobs) && meanshiftJobs !== 0) {
    env.REKEP_MEANSHIFT_N_JOBS = String(meanshiftJobs);
  }
  return env;
}

function buildBridgeInvocation(cfg, params) {
  const action = readString(params.action, "preflight");
  const realAction = isRealAction(action);
  const defaultTaskForAction = realAction ? "generic" : cfg.defaultTask;
  const task = readString(params.task, defaultTaskForAction);
  const useCachedQuery =
    params.useCachedQuery === undefined
      ? cfg.defaultUseCachedQuery
      : readBoolean(params.useCachedQuery, cfg.defaultUseCachedQuery);
  const holdUiSeconds =
    params.holdUiSeconds === undefined
      ? cfg.defaultHoldUiSeconds
      : readNumber(params.holdUiSeconds, cfg.defaultHoldUiSeconds);
  const headless =
    params.headless === undefined
      ? cfg.defaultHeadless
      : readBoolean(params.headless, cfg.defaultHeadless);
  const cameraId =
    params.cameraId === undefined
      ? cfg.defaultSceneQaCameraId
      : readNumber(params.cameraId, cfg.defaultSceneQaCameraId);
  const jobStateDir = readString(params.jobStateDir);
  const realStateDir = readString(params.realStateDir, jobStateDir);
  const cameraSource =
    params.cameraSource === undefined
      ? cfg.defaultRealCameraSource
      : readString(params.cameraSource, cfg.defaultRealCameraSource);
  const cameraProfile =
    params.cameraProfile === undefined
      ? cfg.defaultRealCameraProfile
      : readString(params.cameraProfile, cfg.defaultRealCameraProfile);
  const cameraSerial =
    params.cameraSerial === undefined
      ? cfg.defaultRealCameraSerial
      : readString(params.cameraSerial, cfg.defaultRealCameraSerial);
  const dobotSettingsIni =
    params.dobotSettingsIni === undefined
      ? cfg.defaultRealDobotSettingsIni
      : readString(params.dobotSettingsIni, cfg.defaultRealDobotSettingsIni);
  const cameraExtrinsicScript =
    params.cameraExtrinsicScript === undefined
      ? cfg.defaultRealCameraExtrinsicScript
      : readString(params.cameraExtrinsicScript, cfg.defaultRealCameraExtrinsicScript);
  const realsenseCalibDir =
    params.realsenseCalibDir === undefined
      ? cfg.defaultRealRealsenseCalibDir
      : readString(params.realsenseCalibDir, cfg.defaultRealRealsenseCalibDir);
  const cameraTimeoutS =
    params.cameraTimeoutS === undefined
      ? cfg.defaultRealCameraTimeoutS
      : readNumber(params.cameraTimeoutS, cfg.defaultRealCameraTimeoutS);
  const cameraWarmupFrames =
    params.cameraWarmupFrames === undefined
      ? cfg.defaultRealCameraWarmupFrames
      : readNumber(params.cameraWarmupFrames, cfg.defaultRealCameraWarmupFrames);
  const standbyStaleTimeoutS =
    params.standbyStaleTimeoutS === undefined
      ? cfg.defaultRealStandbyStaleTimeoutS
      : readNumber(params.standbyStaleTimeoutS, cfg.defaultRealStandbyStaleTimeoutS);
  const useStandbyFrame =
    params.useStandbyFrame === undefined
      ? cfg.defaultRealUseStandbyFrame
      : readBoolean(params.useStandbyFrame, cfg.defaultRealUseStandbyFrame);
  const dobotDriver =
    params.dobotDriver === undefined
      ? cfg.defaultRealDobotDriver
      : readString(params.dobotDriver, cfg.defaultRealDobotDriver);
  const dobotHost =
    params.dobotHost === undefined
      ? cfg.defaultRealDobotHost
      : readString(params.dobotHost, cfg.defaultRealDobotHost);
  const dobotPort =
    params.dobotPort === undefined
      ? cfg.defaultRealDobotPort
      : readNumber(params.dobotPort, cfg.defaultRealDobotPort);
  const intervalS =
    params.intervalS === undefined
      ? cfg.defaultRealIntervalS
      : readNumber(params.intervalS, cfg.defaultRealIntervalS);
  const dobotMovePort =
    params.dobotMovePort === undefined
      ? cfg.defaultRealDobotMovePort
      : readNumber(params.dobotMovePort, cfg.defaultRealDobotMovePort);
  const executeMotion =
    params.executeMotion === undefined
      ? cfg.defaultRealExecuteMotion
      : readBoolean(params.executeMotion, cfg.defaultRealExecuteMotion);
  const model =
    params.model === undefined
      ? cfg.defaultRealModel
      : readString(params.model, cfg.defaultRealModel);
  const temperature =
    params.temperature === undefined
      ? cfg.defaultRealTemperature
      : readNumber(params.temperature, cfg.defaultRealTemperature);
  const maxTokens =
    params.maxTokens === undefined
      ? cfg.defaultRealMaxTokens
      : readNumber(params.maxTokens, cfg.defaultRealMaxTokens);
  const maxRuntimeMinutes =
    params.maxRuntimeMinutes === undefined
      ? cfg.defaultRealLongrunMaxRuntimeMinutes
      : readNumber(params.maxRuntimeMinutes, cfg.defaultRealLongrunMaxRuntimeMinutes);
  const monitorIntervalS =
    params.monitorIntervalS === undefined
      ? cfg.defaultRealLongrunMonitorIntervalS
      : readNumber(params.monitorIntervalS, cfg.defaultRealLongrunMonitorIntervalS);
  const retryLimit =
    params.retryLimit === undefined
      ? cfg.defaultRealLongrunRetryLimit
      : readNumber(params.retryLimit, cfg.defaultRealLongrunRetryLimit);
  const feishuWebhook =
    params.feishuWebhook === undefined
      ? cfg.defaultRealFeishuWebhook
      : readString(params.feishuWebhook, cfg.defaultRealFeishuWebhook);
  const xtrainerSdkDir =
    params.xtrainerSdkDir === undefined
      ? cfg.defaultRealXtrainerSdkDir
      : readString(params.xtrainerSdkDir, cfg.defaultRealXtrainerSdkDir);
  const rekepExecutionMode =
    readString(
      params.rekepExecutionMode === undefined
        ? cfg.defaultRealRekepExecutionMode
        : params.rekepExecutionMode,
      cfg.defaultRealRekepExecutionMode,
    ).toLowerCase() === "vlm_stage"
      ? "vlm_stage"
      : "solver";
  const meanshiftJobs =
    params.meanshiftJobs === undefined
      ? cfg.defaultRealMeanshiftJobs
      : readNumber(params.meanshiftJobs, cfg.defaultRealMeanshiftJobs);
  let normalizedCameraSource = cameraSource;
  if (
    realAction &&
    readString(dobotDriver).toLowerCase() === "xtrainer_zmq" &&
    isPlainRealsenseSource(cameraSource)
  ) {
    // Guard against LLM/tool calls that accidentally request local RealSense while using remote ZMQ robot mode.
    normalizedCameraSource = buildRemoteRealsenseSource(dobotHost || cfg.defaultRealDobotHost);
  }

  const argv = [cfg.bridgeScriptPath, action];
  if (task) {
    argv.push("--task", task);
  }
  if (jobStateDir && !realAction) {
    argv.push("--job_state_dir", jobStateDir);
  }
  if (realAction && realStateDir) {
    argv.push("--real_state_dir", realStateDir);
  }
  if (readString(params.jobId)) {
    argv.push("--job_id", readString(params.jobId));
  }
  if ((action === "scene_qa" || action === "real_scene_qa") && readString(params.question)) {
    argv.push("--question", readString(params.question));
  }
  if (readString(params.instruction)) {
    argv.push("--instruction", readString(params.instruction));
  }
  if (readString(params.command)) {
    argv.push("--command", readString(params.command));
  }
  if (readString(params.commandText)) {
    argv.push("--command_text", readString(params.commandText));
  }
  if (readString(params.sceneFile)) {
    argv.push("--scene_file", readString(params.sceneFile));
  }
  if (readString(params.rekepProgramDir)) {
    argv.push("--rekep_program_dir", readString(params.rekepProgramDir));
  }
  if (!realAction) {
    argv.push(headless ? "--headless" : "--no-headless");
  }
  if (!realAction && action !== "scene_qa" && useCachedQuery) {
    argv.push("--use_cached_query");
  }
  if (!realAction && action === "scene_qa" && Number.isFinite(cameraId)) {
    argv.push("--camera_id", String(cameraId));
  }
  if (!realAction && action !== "scene_qa" && readBoolean(params.applyDisturbance, false)) {
    argv.push("--apply_disturbance");
  }
  if (!realAction && action !== "scene_qa" && readBoolean(params.visualize, false)) {
    argv.push("--visualize");
  }
  if (!realAction && action !== "scene_qa" && !headless && holdUiSeconds > 0) {
    argv.push("--hold_ui_seconds", String(holdUiSeconds));
  }
  if (realAction && readString(normalizedCameraSource)) {
    argv.push("--camera_source", readString(normalizedCameraSource));
  }
  if (realAction && readString(cameraProfile)) {
    argv.push("--camera_profile", readString(cameraProfile));
  }
  if (realAction && readString(cameraSerial)) {
    argv.push("--camera_serial", readString(cameraSerial));
  }
  if (realAction && readString(dobotSettingsIni)) {
    argv.push("--dobot_settings_ini", readString(dobotSettingsIni));
  }
  if (realAction && readString(cameraExtrinsicScript)) {
    argv.push("--camera_extrinsic_script", readString(cameraExtrinsicScript));
  }
  if (realAction && readString(realsenseCalibDir)) {
    argv.push("--realsense_calib_dir", readString(realsenseCalibDir));
  }
  if (realAction && Number.isFinite(cameraTimeoutS)) {
    argv.push("--camera_timeout_s", String(cameraTimeoutS));
  }
  if (realAction && Number.isFinite(cameraWarmupFrames)) {
    argv.push("--camera_warmup_frames", String(Math.max(1, Math.trunc(cameraWarmupFrames))));
  }
  if (realAction && Number.isFinite(standbyStaleTimeoutS)) {
    argv.push("--standby_stale_timeout_s", String(standbyStaleTimeoutS));
  }
  if (realAction && useStandbyFrame) {
    argv.push("--use_standby_frame");
  }
  if (realAction && readString(dobotDriver)) {
    argv.push("--dobot_driver", readString(dobotDriver));
  }
  if (realAction && readString(dobotHost)) {
    argv.push("--dobot_host", readString(dobotHost));
  }
  if (realAction && Number.isFinite(dobotPort)) {
    argv.push("--dobot_port", String(Math.trunc(dobotPort)));
  }
  if (realAction && Number.isFinite(dobotMovePort)) {
    argv.push("--dobot_move_port", String(Math.trunc(dobotMovePort)));
  }
  if (realAction && readString(xtrainerSdkDir)) {
    argv.push("--xtrainer_sdk_dir", readString(xtrainerSdkDir));
  }
  if (realAction && Number.isFinite(intervalS)) {
    argv.push("--interval_s", String(intervalS));
  }
  if (realAction && executeMotion) {
    argv.push("--execute_motion");
  }
  if (realAction && readString(model)) {
    argv.push("--model", readString(model));
  }
  if (realAction && Number.isFinite(temperature)) {
    argv.push("--temperature", String(temperature));
  }
  if (realAction && Number.isFinite(maxTokens)) {
    argv.push("--max_tokens", String(Math.max(1, Math.trunc(maxTokens))));
  }
  if (realAction && readString(rekepExecutionMode)) {
    argv.push("--rekep_execution_mode", rekepExecutionMode);
  }
  if (realAction && Number.isFinite(maxRuntimeMinutes)) {
    argv.push("--max_runtime_minutes", String(Math.max(1, maxRuntimeMinutes)));
  }
  if (realAction && Number.isFinite(monitorIntervalS)) {
    argv.push("--monitor_interval_s", String(Math.max(0.2, monitorIntervalS)));
  }
  if (realAction && Number.isFinite(retryLimit)) {
    argv.push("--retry_limit", String(Math.max(0, Math.trunc(retryLimit))));
  }
  if (realAction && readString(feishuWebhook)) {
    argv.push("--feishu_webhook", readString(feishuWebhook));
  }
  if (readBoolean(params.force, false)) {
    argv.push("--force");
  }

  const envExtras = { jobStateDir, rekepExecutionMode, meanshiftJobs };
  if (cfg.pythonBin) {
    return { command: cfg.pythonBin, argv, env: buildBridgeEnv(cfg, envExtras) };
  }

  return {
    command: cfg.condaExe,
    argv: ["run", "--no-capture-output", "-n", cfg.condaEnv, "python", ...argv],
    env: buildBridgeEnv(cfg, envExtras),
  };
}

async function runSubprocess(params) {
  return await new Promise((resolve, reject) => {
    const child = spawn(params.command, params.argv, {
      cwd: params.cwd,
      stdio: ["ignore", "pipe", "pipe"],
      env: params.env ?? process.env,
      windowsHide: true,
    });

    let stdout = "";
    let stderrTail = "";
    let liveOutput = "";
    let stderrLineRemainder = "";
    let stdoutBytes = 0;
    let settled = false;
    let updateTimer = null;
    let lastUpdateText = "";
    const progress = createProgressState(params.action);

    const pushTail = (current, chunk, maxChars) => {
      const next = `${current}${chunk}`;
      return next.length <= maxChars ? next : next.slice(-maxChars);
    };

    const buildRunningUpdate = () => buildProgressUpdateText(progress);

    const emitUpdate = (force = false) => {
      if (!params.onUpdate) {
        return;
      }
      const text = buildRunningUpdate();
      if (!force && text === lastUpdateText) {
        return;
      }
      lastUpdateText = text;
      params.onUpdate({
        content: [{ type: "text", text }],
        details: {
          status: "running",
          command: params.command,
          argv: params.argv,
          progress: { ...progress },
        },
      });
    };

    const scheduleUpdate = () => {
      if (!params.onUpdate || updateTimer !== null) {
        return;
      }
      updateTimer = setTimeout(() => {
        updateTimer = null;
        emitUpdate();
      }, LIVE_UPDATE_THROTTLE_MS);
    };

    const settle = (err, result) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      if (updateTimer !== null) {
        clearTimeout(updateTimer);
        updateTimer = null;
      }
      params.signal?.removeEventListener("abort", handleAbort);
      if (err) {
        reject(err);
      } else {
        resolve(result);
      }
    };

    const failAndTerminate = (message) => {
      const error = new Error(message);
      error.stdout = stdout;
      error.stderrTail = stderrTail;
      error.liveOutput = liveOutput;
      error.progress = { ...progress };
      try {
        child.kill("SIGKILL");
      } finally {
        settle(error);
      }
    };

    const handleAbort = () => {
      failAndTerminate("rekep bridge aborted");
    };

    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");

    child.stdout?.on("data", (chunk) => {
      const text = String(chunk);
      stdoutBytes += Buffer.byteLength(text, "utf8");
      if (stdoutBytes > params.maxStdoutBytes) {
        failAndTerminate("rekep bridge output exceeded maxStdoutBytes");
        return;
      }
      stdout += text;
    });

    child.stderr?.on("data", (chunk) => {
      const text = String(chunk);
      stderrTail = pushTail(stderrTail, text, DEFAULT_STDERR_TAIL_CHARS);
      liveOutput = pushTail(liveOutput, text, DEFAULT_LIVE_OUTPUT_CHARS);
      const merged = `${stderrLineRemainder}${text}`;
      const lines = merged.split(/\r?\n/);
      stderrLineRemainder = lines.pop() ?? "";
      for (const line of lines) {
        updateProgressFromLine(progress, line);
      }
      scheduleUpdate();
    });

    const timer = setTimeout(() => {
      failAndTerminate("rekep bridge timed out");
    }, params.timeoutMs);

    if (params.signal?.aborted) {
      failAndTerminate("rekep bridge aborted");
      return;
    }
    params.signal?.addEventListener("abort", handleAbort, { once: true });

    emitUpdate(true);
    child.once("error", (err) => settle(err));
    child.once("exit", (code) => {
      if (readString(stderrLineRemainder)) {
        updateProgressFromLine(progress, stderrLineRemainder);
      }
      const normalizedCode = code ?? 0;
      if (normalizedCode === 0) {
        progress.stage = "Completed";
        progress.status = "Bridge subprocess finished successfully.";
        progress.nextStep = "Prepare final response.";
      } else {
        progress.stage = "Failed";
        progress.status = `Bridge subprocess exited with code ${normalizedCode}.`;
        progress.nextStep = "Return failure response.";
      }
      progress.updatedAtMs = Date.now();
      emitUpdate(true);
      settle(null, { stdout, stderrTail, liveOutput, progress: { ...progress }, exitCode: normalizedCode });
    });
  });
}

function parseJsonPayload(stdout) {
  const trimmed = stdout.trim();
  if (!trimmed) {
    throw new Error("rekep bridge returned empty output");
  }
  try {
    return JSON.parse(trimmed);
  } catch {
    const suffixMatch = trimmed.match(/({[\s\S]*}|\[[\s\S]*])\s*$/);
    if (suffixMatch?.[1]) {
      return JSON.parse(suffixMatch[1]);
    }
    throw new Error("rekep bridge returned invalid JSON");
  }
}

function buildFailurePayload(action, task, error, extras = {}) {
  const payload = {
    ok: false,
    action: readString(action, "unknown"),
    error: readString(error, "unknown error"),
  };
  const taskName = readString(task);
  if (taskName) {
    payload.task = taskName;
  }
  return { ...payload, ...extras };
}

function extractRunLogPath(...texts) {
  for (const text of texts) {
    const value = readString(text);
    if (!value) {
      continue;
    }
    const match = value.match(/\[bridge\]\s+run_log_path:\s*(\S+)/);
    if (match?.[1]) {
      return match[1];
    }
  }
  return "";
}

function extractVlmTracePath(...texts) {
  for (const text of texts) {
    const value = readString(text);
    if (!value) {
      continue;
    }
    const match = value.match(/\[vlm\]\s+trace_path:\s*(\S+)/);
    if (match?.[1]) {
      return match[1];
    }
  }
  return "";
}

async function readJsonIfExists(filePath) {
  const value = readString(filePath);
  if (!value) {
    return null;
  }
  try {
    const raw = await fs.readFile(value, "utf8");
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function readFileTail(filePath, maxChars = DEFAULT_LIVE_OUTPUT_CHARS) {
  const value = readString(filePath);
  if (!value) {
    return "";
  }
  try {
    const raw = await fs.readFile(value, "utf8");
    return raw.length <= maxChars ? raw : raw.slice(-maxChars);
  } catch {
    return "";
  }
}

async function sleep(ms, signal) {
  if (signal?.aborted) {
    throw new Error("rekep background monitor aborted");
  }
  return await new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", handleAbort);
      resolve();
    }, ms);
    const handleAbort = () => {
      clearTimeout(timer);
      reject(new Error("rekep background monitor aborted"));
    };
    signal?.addEventListener("abort", handleAbort, { once: true });
  });
}

function buildBackgroundMonitorText(progress) {
  return buildProgressUpdateText(progress);
}

async function monitorBackgroundLiveRun(payload, signal, onUpdate) {
  const job = asObject(payload.job);
  const statusPath = readString(job.status_path, readString(job.state_path));
  if (!statusPath || !onUpdate) {
    return payload;
  }

  let latestJob = job;
  let latestTail = "";
  let lastUpdateText = "";
  let sawVlmResponse = false;
  const jobStartMs =
    parseIsoTimestampMs(job.started_at) ?? parseIsoTimestampMs(job.created_at) ?? Date.now();
  let latestProgress = createProgressState(readString(payload.action, "run_background"));
  latestProgress.startedAtMs = jobStartMs;
  latestProgress.updatedAtMs = Date.now();
  latestProgress.runLogPath = readString(latestJob.run_log_path, latestProgress.runLogPath);
  const deadline = Date.now() + BACKGROUND_MONITOR_MAX_MS;

  while (Date.now() < deadline) {
    const state = asObject(await readJsonIfExists(statusPath));
    if (Object.keys(state).length > 0) {
      latestJob = state;
    }
    latestTail = await readFileTail(readString(latestJob.run_log_path, job.run_log_path));
    latestProgress = createProgressState(readString(payload.action, "run_background"));
    latestProgress.startedAtMs =
      parseIsoTimestampMs(latestJob.started_at) ??
      parseIsoTimestampMs(latestJob.created_at) ??
      jobStartMs;
    latestProgress.updatedAtMs = Date.now();
    latestProgress.runLogPath = readString(latestJob.run_log_path, job.run_log_path);
    applyProgressFromText(latestProgress, latestTail);
    const text = buildBackgroundMonitorText(latestProgress);
    if (text && text !== lastUpdateText) {
      lastUpdateText = text;
      onUpdate({
        content: [{ type: "text", text }],
        details: {
          status: "running",
          job: latestJob,
          progress: { ...latestProgress },
        },
      });
    }
    if (
      latestTail.includes("[vlm] assistant_response_end") ||
      latestTail.includes("[vlm] trace_path:") ||
      latestTail.includes("[vlm] planner_raw_end") ||
      latestTail.includes("[vlm] plan_raw_path:") ||
      readString(latestProgress.tracePath) ||
      readString(latestProgress.planRawPath)
    ) {
      sawVlmResponse = true;
    }
    const status = readString(latestJob.status);
    if (["succeeded", "failed", "cancelled"].includes(status) || sawVlmResponse) {
      break;
    }
    await sleep(BACKGROUND_MONITOR_POLL_MS, signal);
  }

  const vlmTracePath = extractVlmTracePath(latestTail);
  return {
    ...payload,
    job: latestJob,
    log_tail: "",
    monitoring: {
      sawVlmResponse,
      vlmTracePath,
      progress: latestProgress,
    },
  };
}

function formatResultMessage(payload, options = {}) {
  const action = readString(payload.action, "unknown");
  const ok = payload.ok === true;
  const includeLiveLogTail = readBoolean(options.includeLiveLogTail, false);
  const liveOutput = includeLiveLogTail ? readString(options.liveOutput) : "";
  const readTail = (value) => (includeLiveLogTail ? readString(value) : "");

  if (action === "preflight") {
    const preflight = asObject(payload.preflight);
    const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
    const display = asObject(preflight.display);
    const mode =
      typeof display.headless_requested === "boolean"
        ? `\nMode: ${display.headless_requested ? "headless" : "gui"}`
        : "";
    return ok
      ? `ReKep preflight: ${readString(preflight.status, "unknown")}${mode}\nBlockers: ${blockers.length > 0 ? blockers.join(", ") : "none"}`
      : `ReKep preflight failed: ${readString(payload.error, "unknown error")}`;
  }

  if (action === "list_tasks") {
    const tasks = Array.isArray(payload.tasks) ? payload.tasks.join(", ") : "";
    return `ReKep tasks: ${tasks || "none"}`;
  }

  if (action === "real_preflight") {
    const preflight = asObject(payload.preflight);
    const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
    const lines = [
      ok
        ? `ReKep real preflight: ${readString(preflight.status, "unknown")}`
        : `ReKep real preflight failed: ${readString(payload.error, "unknown error")}`,
    ];
    if (readString(preflight.driver)) {
      lines.push(`Driver: ${readString(preflight.driver)}`);
    }
    if (readString(preflight.camera_source)) {
      lines.push(`Camera source: ${readString(preflight.camera_source)}`);
    }
    if (typeof preflight.execute_motion === "boolean") {
      lines.push(`Execute motion: ${preflight.execute_motion ? "true" : "false"}`);
    }
    if (readString(asObject(preflight.vlm).model)) {
      lines.push(`VLM: ${readString(asObject(preflight.vlm).model)}`);
    }
    lines.push(`Blockers: ${blockers.length > 0 ? blockers.join(", ") : "none"}`);
    return lines.join("\n");
  }

  if (action === "real_standby_start" || action === "real_standby_status" || action === "real_standby_stop") {
    if (!ok) {
      return `ReKep real standby failed: ${readString(payload.error, "unknown error")}`;
    }
    const standby = asObject(payload.standby);
    const lines = [
      `ReKep real standby status: ${readString(standby.status, "unknown")}.`,
    ];
    if (readString(standby.camera_source)) {
      lines.push(`Camera source: ${readString(standby.camera_source)}`);
    }
    if (typeof standby.worker_alive === "boolean") {
      lines.push(`Worker alive: ${standby.worker_alive ? "true" : "false"}`);
    }
    if (readString(standby.latest_frame_path)) {
      lines.push(`Latest frame: ${readString(standby.latest_frame_path)}`);
    }
    if (readString(standby.error)) {
      lines.push(`Error: ${readString(standby.error)}`);
    }
    return lines.join("\n");
  }

  if (action === "real_scene_qa") {
    if (!ok) {
      const preflight = asObject(payload.preflight);
      const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
      const lines = [`ReKep real scene QA failed: ${readString(payload.error, "unknown error")}`];
      if (blockers.length > 0) {
        lines.push(`Blockers: ${blockers.join(", ")}`);
      }
      if (readString(payload.run_log_path)) {
        lines.push(`Log: ${readString(payload.run_log_path)}`);
      }
      if (liveOutput) {
        lines.push("", "Recent log tail:", liveOutput);
      }
      return lines.join("\n");
    }
    const result = asObject(payload.result);
    const lines = ["ReKep real scene QA finished."];
    if (readString(result.question)) {
      lines.push(`Question: ${readString(result.question)}`);
    }
    if (readString(result.answer)) {
      lines.push(`Answer: ${readString(result.answer)}`);
    }
    if (readString(result.snapshot_path)) {
      lines.push(`Snapshot: ${readString(result.snapshot_path)}`);
    }
    if (readString(result.vlm_trace_path)) {
      lines.push(`VLM Trace: ${readString(result.vlm_trace_path)}`);
    }
    if (readString(asObject(result.vlm).model)) {
      lines.push(`VLM: ${readString(asObject(result.vlm).model)}`);
    }
    if (readString(payload.run_log_path)) {
      lines.push(`Log: ${readString(payload.run_log_path)}`);
    }
    if (liveOutput) {
      lines.push("", "Recent log tail:", liveOutput);
    }
    return lines.join("\n");
  }

  if (action === "real_execute") {
    const result = asObject(payload.result);
    const executionRecords = Array.isArray(result.execution_records) ? result.execution_records : [];
    const plannedActions = Array.isArray(result.plan_actions) ? result.plan_actions : [];
    const stageResults = Array.isArray(result.stage_results) ? result.stage_results : [];
    const plannedFromStages = stageResults.reduce((count, stage) => {
      const planActions = Array.isArray(asObject(stage).plan_actions) ? asObject(stage).plan_actions : [];
      return count + planActions.length;
    }, 0);
    const executedFromStages = stageResults.reduce((count, stage) => {
      const records = Array.isArray(asObject(stage).execution_records) ? asObject(stage).execution_records : [];
      return count + records.length;
    }, 0);
    const plannedCount = stageResults.length > 0 ? plannedFromStages : plannedActions.length;
    const executedCount = stageResults.length > 0 ? executedFromStages : executionRecords.length;
    const lines = [
      ok
        ? "ReKep real execute finished."
        : `ReKep real execute failed: ${readString(payload.error, "unknown error")}`,
    ];
    if (readString(result.instruction)) {
      lines.push(`Instruction: ${readString(result.instruction)}`);
    }
    if (readString(result.driver)) {
      lines.push(`Driver: ${readString(result.driver)}`);
    }
    if (typeof result.execute_motion === "boolean") {
      lines.push(`Execute motion: ${result.execute_motion ? "true" : "false"}`);
    }
    if (readString(result.frame_path)) {
      lines.push(`Frame: ${readString(result.frame_path)}`);
    }
    if (stageResults.length > 0) {
      lines.push(`Stages: ${stageResults.length}`);
    }
    if (Number.isFinite(plannedCount)) {
      lines.push(`Planned actions: ${plannedCount}`);
    }
    if (Number.isFinite(executedCount)) {
      lines.push(`Executed steps: ${executedCount}`);
    }
    if (readString(result.plan_raw_output_path)) {
      lines.push(`Plan raw output: ${readString(result.plan_raw_output_path)}`);
    }
    if (readString(payload.run_log_path)) {
      lines.push(`Log: ${readString(payload.run_log_path)}`);
    }
    if (liveOutput) {
      lines.push("", "Recent log tail:", liveOutput);
    }
    return lines.join("\n");
  }

  if (action === "real_execute_background") {
    if (!ok) {
      const lines = [`ReKep real background execute failed: ${readString(payload.error, "unknown error")}`];
      if (readString(asObject(payload.job).run_log_path)) {
        lines.push(`Log: ${readString(asObject(payload.job).run_log_path)}`);
      }
      return lines.join("\n");
    }
    const job = asObject(payload.job);
    const lines = ["ReKep real background execute started."];
    if (readString(job.job_id)) {
      lines.push(`Job: ${readString(job.job_id)}`);
    }
    if (readString(job.instruction)) {
      lines.push(`Instruction: ${readString(job.instruction)}`);
    }
    if (readString(job.driver)) {
      lines.push(`Driver: ${readString(job.driver)}`);
    }
    if (typeof job.execute_motion === "boolean") {
      lines.push(`Execute motion: ${job.execute_motion ? "true" : "false"}`);
    }
    if (readString(job.status_path, readString(job.state_path))) {
      lines.push(`Status file: ${readString(job.status_path, readString(job.state_path))}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    lines.push("Check progress by asking in chat: 查询当前机械臂任务进度。");
    return lines.join("\n");
  }

  if (action === "real_job_status") {
    if (!ok) {
      return `ReKep real job status failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const result = asObject(job.result);
    const lines = [
      `ReKep real job ${readString(job.job_id, "unknown")} is ${readString(job.status, "unknown")}.`,
    ];
    if (readString(job.instruction)) {
      lines.push(`Instruction: ${readString(job.instruction)}`);
    }
    if (readString(job.driver)) {
      lines.push(`Driver: ${readString(job.driver)}`);
    }
    if (typeof job.execute_motion === "boolean") {
      lines.push(`Execute motion: ${job.execute_motion ? "true" : "false"}`);
    }
    if (typeof job.worker_alive === "boolean") {
      lines.push(`Worker alive: ${job.worker_alive ? "true" : "false"}`);
    }
    if (readString(job.error)) {
      lines.push(`Error: ${readString(job.error)}`);
    }
    if (readString(result.frame_path)) {
      lines.push(`Frame: ${readString(result.frame_path)}`);
    }
    if (readString(result.plan_raw_output_path)) {
      lines.push(`Plan raw output: ${readString(result.plan_raw_output_path)}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    return lines.join("\n");
  }

  if (action === "real_job_cancel") {
    if (!ok) {
      return `ReKep real job cancel failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const lines = [
      `ReKep real job ${readString(job.job_id, "unknown")} is ${readString(job.status, "cancelled")}.`,
    ];
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    return lines.join("\n");
  }

  if (action === "real_longrun_start") {
    if (!ok) {
      return `ReKep real longrun start failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const lines = ["ReKep real longrun started."];
    if (readString(job.job_id)) {
      lines.push(`Job: ${readString(job.job_id)}`);
    }
    if (readString(job.instruction)) {
      lines.push(`Goal: ${readString(job.instruction)}`);
    }
    if (Array.isArray(job.pending_subtasks)) {
      lines.push(`Pending subtasks: ${job.pending_subtasks.length}`);
    }
    if (typeof job.execute_motion === "boolean") {
      lines.push(`Execute motion: ${job.execute_motion ? "true" : "false"}`);
    }
    if (readString(job.status_path, readString(job.state_path))) {
      lines.push(`Status file: ${readString(job.status_path, readString(job.state_path))}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    lines.push("Human intervention: use real_longrun_command with pause/resume/replace_current/replace_plan/skip_current.");
    return lines.join("\n");
  }

  if (action === "real_longrun_status") {
    if (!ok) {
      return `ReKep real longrun status failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const lines = [
      `ReKep real longrun ${readString(job.job_id, "unknown")} is ${readString(job.status, "unknown")}.`,
    ];
    if (readString(job.phase)) {
      lines.push(`Phase: ${readString(job.phase)}`);
    }
    if (typeof job.worker_alive === "boolean") {
      lines.push(`Worker alive: ${job.worker_alive ? "true" : "false"}`);
    }
    if (typeof job.paused === "boolean") {
      lines.push(`Paused: ${job.paused ? "true" : "false"}`);
    }
    if (readString(job.current_subtask)) {
      lines.push(`Current subtask: ${readString(job.current_subtask)}`);
    }
    if (Array.isArray(job.pending_subtasks)) {
      lines.push(`Pending subtasks: ${job.pending_subtasks.length}`);
    }
    if (Array.isArray(job.completed_subtasks)) {
      lines.push(`Completed subtasks: ${job.completed_subtasks.length}`);
    }
    if (Array.isArray(job.failed_subtasks)) {
      lines.push(`Failed subtasks: ${job.failed_subtasks.length}`);
    }
    if (Number.isFinite(job.pending_commands)) {
      lines.push(`Pending commands: ${job.pending_commands}`);
    }
    if (readString(job.error)) {
      lines.push(`Error: ${readString(job.error)}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    return lines.join("\n");
  }

  if (action === "real_longrun_command") {
    if (!ok) {
      return `ReKep real longrun command failed: ${readString(payload.error, "unknown error")}`;
    }
    const event = asObject(payload.command_event);
    const job = asObject(payload.job);
    const lines = [
      `ReKep longrun command queued: ${readString(event.command, "unknown")}.`,
    ];
    if (readString(event.text)) {
      lines.push(`Text: ${readString(event.text)}`);
    }
    if (readString(job.job_id)) {
      lines.push(`Job: ${readString(job.job_id)}`);
    }
    if (readString(job.current_subtask)) {
      lines.push(`Current subtask: ${readString(job.current_subtask)}`);
    }
    return lines.join("\n");
  }

  if (action === "real_longrun_stop") {
    if (!ok) {
      return `ReKep real longrun stop failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const lines = [
      `ReKep real longrun ${readString(job.job_id, "unknown")} is ${readString(job.status, "cancelled")}.`,
    ];
    if (readString(job.error)) {
      lines.push(`Reason: ${readString(job.error)}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    return lines.join("\n");
  }

  if (action === "run") {
    if (!ok) {
      const preflight = asObject(payload.preflight);
      const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
      const display = asObject(preflight.display);
      const lines = [
        `ReKep run failed for task ${readString(payload.task, "unknown")}: ${readString(payload.error, "unknown error")}`,
      ];
      if (typeof display.headless_requested === "boolean") {
        lines.push(`Mode: ${display.headless_requested ? "headless" : "gui"}`);
      }
      if (blockers.length > 0) {
        lines.push(`Blockers: ${blockers.join(", ")}`);
      }
      if (readString(payload.run_log_path)) {
        lines.push(`Log: ${readString(payload.run_log_path)}`);
      }
      if (liveOutput) {
        lines.push("", "Recent log tail:", liveOutput);
      }
      return lines.join("\n");
    }
    const result = asObject(payload.result);
    const mode =
      typeof asObject(payload.preflight).display?.headless_requested === "boolean"
        ? (asObject(payload.preflight).display.headless_requested ? "headless" : "gui")
        : "";
    const lines = [
      `ReKep run finished for task ${readString(result.task, "unknown")}.`,
    ];
    if (mode) {
      lines.push(`Mode: ${mode}`);
    }
    if (readString(result.instruction)) {
      lines.push(`Instruction: ${readString(result.instruction)}`);
    }
    if (readString(result.video_path)) {
      lines.push(`Video: ${readString(result.video_path)}`);
    }
    if (readString(result.rekep_program_dir)) {
      lines.push(`Program: ${readString(result.rekep_program_dir)}`);
    }
    for (const [label, key] of [
      ["VLM Prompt", "vlm_prompt_path"],
      ["VLM Raw Output", "vlm_raw_output_path"],
      ["VLM Trace", "vlm_trace_path"],
    ]) {
      const value = readString(result[key]);
      if (value) {
        lines.push(`${label}: ${value}`);
      }
    }
    if (readString(payload.run_log_path)) {
      lines.push(`Log: ${readString(payload.run_log_path)}`);
    }
    if (liveOutput) {
      lines.push("", "Recent log tail:", liveOutput);
    }
    return lines.join("\n");
  }

  if (action === "run_background") {
    if (!ok) {
      const lines = [
        `ReKep background run failed for task ${readString(payload.task, "unknown")}: ${readString(payload.error, "unknown error")}`,
      ];
      const preflight = asObject(payload.preflight);
      const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
      const job = asObject(payload.job);
      if (blockers.length > 0) {
        lines.push(`Blockers: ${blockers.join(", ")}`);
      }
      if (readString(job.run_log_path)) {
        lines.push(`Log: ${readString(job.run_log_path)}`);
      }
      return lines.join("\n");
    }
    const job = asObject(payload.job);
    const lines = [
      `ReKep background run started for task ${readString(job.task, "unknown")}.`,
    ];
    if (typeof job.headless === "boolean") {
      lines.push(`Mode: ${job.headless ? "headless" : "gui"}`);
    }
    if (readString(job.job_id)) {
      lines.push(`Job: ${readString(job.job_id)}`);
    }
    if (readString(job.instruction)) {
      lines.push(`Instruction: ${readString(job.instruction)}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    if (readString(job.status_path)) {
      lines.push(`Status file: ${readString(job.status_path)}`);
    }
    const monitoring = asObject(payload.monitoring);
    if (readString(monitoring.vlmTracePath)) {
      lines.push(`VLM Trace: ${readString(monitoring.vlmTracePath)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    lines.push("Check progress by asking in chat: 查询当前仿真任务进度。");
    return lines.join("\n");
  }

  if (action === "job_status") {
    if (!ok) {
      return `ReKep job status failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const result = asObject(job.result);
    const artifacts = asObject(payload.artifacts);
    const status = readString(job.status, "unknown");
    const lines = [
      `ReKep job ${readString(job.job_id, "unknown")} is ${status}.`,
    ];
    if (typeof job.headless === "boolean") {
      lines.push(`Mode: ${job.headless ? "headless" : "gui"}`);
    }
    if (readString(job.task)) {
      lines.push(`Task: ${readString(job.task)}`);
    }
    if (readString(job.error)) {
      lines.push(`Error: ${readString(job.error)}`);
    }
    if (job.worker_alive === false && status === "running") {
      lines.push("Worker alive: false");
    }
    if (readString(result.video_path)) {
      lines.push(`Video: ${readString(result.video_path)}`);
    }
    if (readString(result.rekep_program_dir)) {
      lines.push(`Program: ${readString(result.rekep_program_dir)}`);
    }
    for (const [label, key] of [
      ["VLM Prompt", "vlm_prompt_path"],
      ["VLM Raw Output", "vlm_raw_output_path"],
      ["VLM Trace", "vlm_trace_path"],
    ]) {
      const value = readString(result[key]) || readString(artifacts[key]);
      if (value) {
        lines.push(`${label}: ${value}`);
      }
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    return lines.join("\n");
  }

  if (action === "job_cancel") {
    if (!ok) {
      return `ReKep job cancel failed: ${readString(payload.error, "unknown error")}`;
    }
    const job = asObject(payload.job);
    const lines = [
      `ReKep job ${readString(job.job_id, "unknown")} is ${readString(job.status, "cancelled")}.`,
    ];
    if (typeof job.headless === "boolean") {
      lines.push(`Mode: ${job.headless ? "headless" : "gui"}`);
    }
    if (readString(job.run_log_path)) {
      lines.push(`Log: ${readString(job.run_log_path)}`);
    }
    const logTail = readTail(payload.log_tail);
    if (logTail) {
      lines.push("", "Recent log tail:", logTail);
    }
    return lines.join("\n");
  }

  if (action === "scene_qa") {
    if (!ok) {
      const preflight = asObject(payload.preflight);
      const blockers = Array.isArray(preflight.blockers) ? preflight.blockers : [];
      const display = asObject(preflight.display);
      const lines = [
        `ReKep scene QA failed for task ${readString(payload.task, "unknown")}: ${readString(payload.error, "unknown error")}`,
      ];
      if (typeof display.headless_requested === "boolean") {
        lines.push(`Mode: ${display.headless_requested ? "headless" : "gui"}`);
      }
      if (blockers.length > 0) {
        lines.push(`Blockers: ${blockers.join(", ")}`);
      }
      if (readString(payload.run_log_path)) {
        lines.push(`Log: ${readString(payload.run_log_path)}`);
      }
      if (liveOutput) {
        lines.push("", "Recent log tail:", liveOutput);
      }
      return lines.join("\n");
    }

    const result = asObject(payload.result);
    const vlm = asObject(result.vlm);
    const mode =
      typeof asObject(payload.preflight).display?.headless_requested === "boolean"
        ? (asObject(payload.preflight).display.headless_requested ? "headless" : "gui")
        : "";
    const lines = [
      `ReKep scene QA finished for task ${readString(result.task, "unknown")}.`,
    ];
    if (mode) {
      lines.push(`Mode: ${mode}`);
    }
    if (readString(result.question)) {
      lines.push(`Question: ${readString(result.question)}`);
    }
    if (readString(result.answer)) {
      lines.push(`Answer: ${readString(result.answer)}`);
    }
    if (readString(result.snapshot_path)) {
      lines.push(`Snapshot: ${readString(result.snapshot_path)}`);
    }
    if (readString(result.vlm_trace_path)) {
      lines.push(`VLM Trace: ${readString(result.vlm_trace_path)}`);
    }
    if (Number.isFinite(result.camera_id)) {
      lines.push(`Camera: ${result.camera_id}`);
    }
    if (readString(vlm.model)) {
      lines.push(`VLM: ${readString(vlm.model)}`);
    }
    if (readString(payload.run_log_path)) {
      lines.push(`Log: ${readString(payload.run_log_path)}`);
    }
    if (liveOutput) {
      lines.push("", "Recent log tail:", liveOutput);
    }
    return lines.join("\n");
  }

  return JSON.stringify(payload, null, 2);
}

async function executeBridgeAction(api, action, rawParams, signal, onUpdate) {
  const cfg = normalizeConfig(api.pluginConfig);
  if (!cfg.repoDir) {
    throw new Error("plugins.entries.rekep.config.repoDir is required");
  }

  const useCachedQuery =
    rawParams.useCachedQuery === undefined
      ? cfg.defaultUseCachedQuery
      : readBoolean(rawParams.useCachedQuery, cfg.defaultUseCachedQuery);
  const requestedAction = readString(action, "preflight");
  const backgroundDefault =
    (requestedAction === "run" && !useCachedQuery) || requestedAction === "real_execute";
  const background =
    rawParams.background === undefined
      ? backgroundDefault
      : readBoolean(rawParams.background, backgroundDefault);
  let effectiveAction = requestedAction;
  if (requestedAction === "run" && background) {
    effectiveAction = "run_background";
  } else if (requestedAction === "real_execute" && background) {
    effectiveAction = "real_execute_background";
  }
  const jobStateDir = resolvePluginJobStateDir(api, cfg);
  const timeoutMs = readNumber(rawParams.timeoutMs, cfg.defaultTimeoutMs);
  const invocation = buildBridgeInvocation(cfg, {
    ...rawParams,
    action: effectiveAction,
    useCachedQuery,
    jobStateDir,
  });
  let stdout = "";
  let stderrTail = "";
  let liveOutput = "";
  let progress = createProgressState(effectiveAction);
  let exitCode = null;
  try {
    ({ stdout, stderrTail, liveOutput, progress, exitCode } = await runSubprocess({
      action: effectiveAction,
      command: invocation.command,
      argv: invocation.argv,
      commandLine: [invocation.command, ...invocation.argv].join(" "),
      cwd: cfg.repoDir,
      timeoutMs,
      maxStdoutBytes: DEFAULT_MAX_STDOUT_BYTES,
      onUpdate,
      signal,
      env: invocation.env,
    }));
  } catch (err) {
    stderrTail = readString(err?.stderrTail);
    liveOutput = readString(err?.liveOutput);
    stdout = readString(err?.stdout);
    progress = asObject(err?.progress);
    const payload = buildFailurePayload(
      effectiveAction,
      rawParams.task ?? cfg.defaultTask,
      err instanceof Error ? err.message : String(err),
      {
        run_log_path: extractRunLogPath(liveOutput, stderrTail, stdout),
      },
    );
    return {
      content: [
        {
          type: "text",
          text: formatResultMessage(payload, { liveOutput }),
        },
      ],
      details: {
        command: invocation.command,
        argv: invocation.argv,
        exitCode,
        stderrTail,
        payload,
        progress,
        stdout,
      },
    };
  }

  let payload;
  try {
    payload = parseJsonPayload(stdout);
  } catch (err) {
    payload = buildFailurePayload(
      effectiveAction,
      rawParams.task ?? cfg.defaultTask,
      exitCode !== 0
        ? `rekep bridge failed (${exitCode ?? "?"}): ${stderrTail.trim() || stdout.trim() || "no output"}`
        : (err instanceof Error ? err.message : String(err)),
      {
        run_log_path: extractRunLogPath(liveOutput, stderrTail, stdout),
      },
    );
  }

  const shouldMonitorBackground =
    ((effectiveAction === "run_background" && !useCachedQuery) ||
      effectiveAction === "real_execute_background") &&
    payload?.ok === true;
  if (shouldMonitorBackground) {
    try {
      payload = await monitorBackgroundLiveRun(payload, signal, onUpdate);
    } catch (err) {
      payload = {
        ...asObject(payload),
        monitoring: {
          error: err instanceof Error ? err.message : String(err),
        },
      };
    }
  }

  return {
    content: [
      {
        type: "text",
        text: formatResultMessage(asObject(payload), { liveOutput }),
      },
    ],
    details: {
      command: invocation.command,
      argv: invocation.argv,
      exitCode,
      stderrTail,
      payload,
      progress,
    },
    };
  }

function createRekepTool(api) {
  return {
    name: "rekep_sim",
    label: "ReKep Runtime",
    description:
      "Run ReKep simulation or real-robot (Dobot/XTrainer) actions through the local Python bridge.",
    parameters: {
      type: "object",
      properties: {
        action: {
          type: "string",
          enum: [
            "preflight",
            "list_tasks",
            "run",
            "run_background",
            "job_status",
            "job_cancel",
            "real_preflight",
            "real_standby_start",
            "real_standby_status",
            "real_standby_stop",
            "real_scene_qa",
            "real_execute",
            "real_execute_background",
            "real_job_status",
            "real_job_cancel",
            "real_longrun_start",
            "real_longrun_status",
            "real_longrun_command",
            "real_longrun_stop",
          ],
          description: "Bridge action to execute.",
        },
        task: {
          type: "string",
          description: "Named task to run. Default: pen.",
        },
        question: {
          type: "string",
          description: "Question for scene QA actions (`scene_qa` or `real_scene_qa`).",
        },
        instruction: {
          type: "string",
          description: "Optional instruction override (required for real_execute / run / real_longrun_start).",
        },
        command: {
          type: "string",
          enum: ["pause", "resume", "stop", "append_subtask", "replace_plan", "replace_current", "skip_current"],
          description: "Longrun human intervention command for real_longrun_command.",
        },
        commandText: {
          type: "string",
          description: "Natural-language payload for longrun command text / updated subtask.",
        },
        sceneFile: {
          type: "string",
          description: "Optional absolute scene file override.",
        },
        rekepProgramDir: {
          type: "string",
          description: "Optional absolute cached program directory override.",
        },
        useCachedQuery: {
          type: "boolean",
          description: "Use cached ReKep constraints instead of live VLM querying.",
        },
        background: {
          type: "boolean",
          description: "Run the job in the background. Defaults to true for live non-cached sim runs and real_execute.",
        },
        jobId: {
          type: "string",
          description: "Background job id for job_status or job_cancel. Defaults to the latest job when omitted.",
        },
        realStateDir: {
          type: "string",
          description: "Optional runtime state directory override for real actions.",
        },
        cameraSource: {
          type: "string",
          description:
            "Real camera source. Use `realsense`, `realsense:<serial>`, `realsense_zmq://<host>:<port>/<topic>`, or a webcam index/path.",
        },
        cameraProfile: {
          type: "string",
          description: "Camera calibration profile name (for example: global3).",
        },
        cameraSerial: {
          type: "string",
          description: "Camera serial used for calibration lookup (for example: CAMERA_SERIAL).",
        },
        dobotSettingsIni: {
          type: "string",
          description: "Path to dobot_settings.ini that defines camera serial aliases.",
        },
        cameraExtrinsicScript: {
          type: "string",
          description: "Path to Python file containing CAMERA_CONFIGS extrinsic matrices (R/T).",
        },
        realsenseCalibDir: {
          type: "string",
          description: "Path to RealSense calibration json directory.",
        },
        cameraTimeoutS: {
          type: "number",
          minimum: 0.1,
          maximum: 30,
          description: "Camera read timeout for real actions (seconds).",
        },
        cameraWarmupFrames: {
          type: "number",
          minimum: 1,
          maximum: 60,
          description: "Warmup frames before saving a real camera frame.",
        },
        standbyStaleTimeoutS: {
          type: "number",
          minimum: 3,
          maximum: 300,
          description: "Mark standby as failed when no frame heartbeat is seen within this duration.",
        },
        useStandbyFrame: {
          type: "boolean",
          description: "Use the latest standby frame instead of capturing a fresh frame in real actions.",
        },
        dobotDriver: {
          type: "string",
          enum: ["xtrainer_zmq", "xtrainer_sdk", "dashboard_tcp", "mock"],
          description: "Real Dobot driver backend.",
        },
        dobotHost: {
          type: "string",
          description: "Robot IP / host for real actions.",
        },
        dobotPort: {
          type: "number",
          minimum: 1,
          maximum: 65535,
          description: "Robot command port for real actions (xtrainer_zmq default 6001, xtrainer_sdk dashboard default 29999).",
        },
        dobotMovePort: {
          type: "number",
          minimum: 1,
          maximum: 65535,
          description: "Motion command port for xtrainer_sdk driver (default 30003).",
        },
        xtrainerSdkDir: {
          type: "string",
          description: "Absolute path to dobot_xtrainer SDK root directory.",
        },
        executeMotion: {
          type: "boolean",
          description: "Whether to execute real robot motion. If false, actions run as dry-run.",
        },
        intervalS: {
          type: "number",
          minimum: 0.05,
          maximum: 10,
          description: "Standby camera refresh interval in seconds.",
        },
        model: {
          type: "string",
          description: "VLM model id for real scene QA / planning.",
        },
        temperature: {
          type: "number",
          minimum: 0,
          maximum: 2,
          description: "VLM temperature for real scene QA / planning.",
        },
        maxTokens: {
          type: "number",
          minimum: 1,
          maximum: 8192,
          description: "VLM max output tokens for real scene QA / planning.",
        },
        rekepExecutionMode: {
          type: "string",
          enum: ["solver", "vlm_stage"],
          description: "ReKep real execute backend: `solver` (DINO-based) or `vlm_stage` (lighter).",
        },
        meanshiftJobs: {
          type: "number",
          minimum: -1,
          maximum: 64,
          description: "Parallel workers for DINO keypoint MeanShift (REKEP_MEANSHIFT_N_JOBS).",
        },
        maxRuntimeMinutes: {
          type: "number",
          minimum: 1,
          maximum: 180,
          description: "Maximum runtime for longrun real task execution.",
        },
        monitorIntervalS: {
          type: "number",
          minimum: 0.2,
          maximum: 60,
          description: "Online monitor loop interval for longrun execution.",
        },
        retryLimit: {
          type: "number",
          minimum: 0,
          maximum: 10,
          description: "Maximum retry count per longrun subtask.",
        },
        feishuWebhook: {
          type: "string",
          description: "Feishu bot webhook URL for longrun fault alerts.",
        },
        applyDisturbance: {
          type: "boolean",
          description: "Apply the built-in disturbance sequence.",
        },
        visualize: {
          type: "boolean",
          description: "Enable ReKep visualization windows.",
        },
        headless: {
          type: "boolean",
          description: "Run the simulator in headless mode. Defaults to true.",
        },
        holdUiSeconds: {
          type: "number",
          minimum: 0,
          maximum: 600,
          description: "Keep the Isaac Sim window open for a short period before shutdown.",
        },
        force: {
          type: "boolean",
          description: "Attempt a run even when preflight reports blockers.",
        },
        timeoutMs: {
          type: "number",
          minimum: 1000,
          maximum: 3600000,
          description: "Subprocess timeout in milliseconds.",
        },
      },
      additionalProperties: false,
    },
    async execute(_toolCallId, rawParams, signal, onUpdate) {
      return executeBridgeAction(api, readString(rawParams.action, "preflight"), rawParams, signal, onUpdate);
    },
  };
}

function createSceneQaTool(api) {
  return {
    name: "rekep_scene_qa",
    label: "ReKep Scene QA",
    description:
      "Capture the current ReKep simulator camera frame and answer questions about visible objects and scene layout with the configured VLM.",
    parameters: {
      type: "object",
      properties: {
        question: {
          type: "string",
          description: "Question to answer from the current simulator camera frame.",
        },
        task: {
          type: "string",
          description: "Named task / scene to load. Default: pen.",
        },
        cameraId: {
          type: "number",
          minimum: 0,
          maximum: 32,
          description: "Camera id to capture from. Default comes from plugin config.",
        },
        instruction: {
          type: "string",
          description: "Optional instruction override for selecting a task scene variant.",
        },
        sceneFile: {
          type: "string",
          description: "Optional absolute scene file override.",
        },
        rekepProgramDir: {
          type: "string",
          description: "Optional program directory override if the task metadata should match another cached program.",
        },
        force: {
          type: "boolean",
          description: "Attempt a scene QA run even when preflight reports blockers.",
        },
        headless: {
          type: "boolean",
          description: "Run the simulator scene capture in headless mode. Defaults to true.",
        },
        timeoutMs: {
          type: "number",
          minimum: 1000,
          maximum: 3600000,
          description: "Subprocess timeout in milliseconds.",
        },
      },
      required: ["question"],
      additionalProperties: false,
    },
    async execute(_toolCallId, rawParams, signal, onUpdate) {
      return executeBridgeAction(api, "scene_qa", rawParams, signal, onUpdate);
    },
  };
}

export default {
  id: "rekep",
  name: "ReKep",
  description: "Local ReKep simulation + real-robot bridge for OpenClaw.",
  configSchema: {
    validate: validatePluginConfig,
    jsonSchema: {
      type: "object",
      properties: {
        repoDir: { type: "string" },
        condaExe: { type: "string" },
        condaEnv: { type: "string" },
        pythonBin: { type: "string" },
        bridgeScript: { type: "string" },
        jobStateDir: { type: "string" },
        defaultTask: { type: "string" },
        defaultUseCachedQuery: { type: "boolean" },
        defaultHeadless: { type: "boolean" },
        defaultHoldUiSeconds: { type: "number" },
        defaultTimeoutMs: { type: "number" },
        defaultSceneQaCameraId: { type: "number" },
        defaultRealCameraSource: { type: "string" },
        defaultRealCameraProfile: { type: "string" },
        defaultRealCameraSerial: { type: "string" },
        defaultRealDobotDriver: { type: "string" },
        defaultRealDobotHost: { type: "string" },
        defaultRealDobotSettingsIni: { type: "string" },
        defaultRealCameraExtrinsicScript: { type: "string" },
        defaultRealRealsenseCalibDir: { type: "string" },
        defaultRealDobotPort: { type: "number" },
        defaultRealDobotMovePort: { type: "number" },
        defaultRealIntervalS: { type: "number" },
        defaultRealExecuteMotion: { type: "boolean" },
        defaultRealUseStandbyFrame: { type: "boolean" },
        defaultRealCameraTimeoutS: { type: "number" },
        defaultRealStandbyStaleTimeoutS: { type: "number" },
        defaultRealCameraWarmupFrames: { type: "number" },
        defaultRealModel: { type: "string" },
        defaultRealRekepExecutionMode: { type: "string" },
        defaultRealTemperature: { type: "number" },
        defaultRealMaxTokens: { type: "number" },
        defaultRealMeanshiftJobs: { type: "number" },
        defaultRealLongrunMaxRuntimeMinutes: { type: "number" },
        defaultRealLongrunMonitorIntervalS: { type: "number" },
        defaultRealLongrunRetryLimit: { type: "number" },
        defaultRealFeishuWebhook: { type: "string" },
        defaultRealXtrainerSdkDir: { type: "string" },
        vlmApiKeyEnv: { type: "string" },
        vlmBaseUrl: { type: "string" },
        vlmModel: { type: "string" },
      },
      additionalProperties: false,
    },
  },
  register(api) {
    api.registerTool(createRekepTool(api), { optional: true });
    api.registerTool(createSceneQaTool(api), { optional: true });
  },
};
