#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import threading
import time
from collections import Counter, deque

import cv2
import pymysql
import requests
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from kafka import KafkaConsumer


RECOGNITION_RESULT_TOPIC = "workshop.recognition_result"
ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
ALARM_RESULT_TOPIC = "workshop.alarm_result"
DEFAULT_GROUP = "workshop-visualization"

_TASKS = {}
_LOCK = threading.Lock()
_RECOGNITIONS = deque(maxlen=200)
_EVENTS = deque(maxlen=500)
_ALARMS = deque(maxlen=500)
_SERVER_STARTED = False


def start_visualize(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    recognition_topic = (data or {}).get("recognition_result_topic") or _config_value(
        "KAFKA", "RECOGNITION_RESULT_TOPIC", RECOGNITION_RESULT_TOPIC)
    abnormal_topic = (data or {}).get("abnormal_event_topic") or _config_value(
        "KAFKA", "ABNORMAL_EVENT_TOPIC", ABNORMAL_EVENT_TOPIC)
    alarm_topic = (data or {}).get("alarm_result_topic") or _config_value(
        "KAFKA", "ALARM_RESULT_TOPIC", ALARM_RESULT_TOPIC)
    camera_source = (data or {}).get("url") or (data or {}).get("camera_url") or _config_value(
        "VISUALIZATION", "CAMERA_URL", os.getenv("VISUALIZATION_CAMERA_URL", "0"))
    port = int(_config_value("VISUALIZATION", "PORT", os.getenv("VISUALIZATION_PORT", 8088)))
    host = _config_value("VISUALIZATION", "HOST", os.getenv("VISUALIZATION_HOST", "127.0.0.1"))
    public_host = _config_value("VISUALIZATION", "PUBLIC_HOST", host)
    dashboard_url = "http://%s:%s/workshop-dashboard?monitor_id=%s" % (public_host, port, monitor_id)

    with _LOCK:
        task = _TASKS.get(monitor_id)
        if task and task.get("status") == "running":
            return _response(monitor_id, dashboard_url, "running")

        task = {
            "monitor_id": monitor_id,
            "recognition_topic": recognition_topic,
            "abnormal_topic": abnormal_topic,
            "alarm_topic": alarm_topic,
            "camera_source": camera_source,
            "status": "running",
            "stop_event": threading.Event(),
        }
        _TASKS[monitor_id] = task
        threading.Thread(target=_consume_topic, args=(task, recognition_topic, _RECOGNITIONS, "recognition"),
                         daemon=True, name="visual-recognition-%s" % monitor_id).start()
        threading.Thread(target=_consume_topic, args=(task, abnormal_topic, _EVENTS, "event"),
                         daemon=True, name="visual-events-%s" % monitor_id).start()
        threading.Thread(target=_consume_topic, args=(task, alarm_topic, _ALARMS, "alarm"),
                         daemon=True, name="visual-alarms-%s" % monitor_id).start()
        _ensure_server(host, port)

    _set_status(monitor_id, "running")
    return _response(monitor_id, dashboard_url, "running")


def _ensure_server(host, port):
    global _SERVER_STARTED
    if _SERVER_STARTED:
        return
    app = _build_app()
    threading.Thread(
        target=lambda: uvicorn.run(app, host=host, port=port, log_level="warning"),
        daemon=True,
        name="workshop-dashboard-server",
    ).start()
    _SERVER_STARTED = True


def _build_app():
    app = FastAPI(title="Workshop Monitoring Dashboard")

    @app.get("/workshop-dashboard", response_class=HTMLResponse)
    def dashboard():
        return HTML

    @app.get("/api/live")
    def live(monitor_id: str = None):
        recognition = _latest(_RECOGNITIONS, monitor_id)
        event = _latest(_EVENTS, monitor_id)
        alarm = _latest(_ALARMS, monitor_id)
        effective_monitor_id = monitor_id or _record_monitor_id(recognition) or _record_monitor_id(event) or _record_monitor_id(alarm)
        return {
            "msg": "success",
            "data": {
                "monitor_id": effective_monitor_id,
                "recognition": recognition,
                "event": event,
                "alarm": alarm,
                "camera_stream_url": "/api/camera-stream?monitor_id=%s" % effective_monitor_id if effective_monitor_id else "/api/camera-stream",
                "updated_at": _now_text(),
            },
        }

    @app.get("/api/events")
    def events(monitor_id: str = None, limit: int = 100):
        return {"msg": "success", "data": _recent(_EVENTS, monitor_id, limit)}

    @app.get("/api/alarms")
    def alarms(monitor_id: str = None, limit: int = 100):
        return {"msg": "success", "data": _recent(_ALARMS, monitor_id, limit)}

    @app.get("/api/stats")
    def stats(monitor_id: str = None):
        events_data = [item for item in list(_EVENTS) if _match_monitor(item, monitor_id)]
        type_counter = Counter()
        level_counter = Counter()
        for event in events_data:
            level_counter[event.get("abnormal_level", "unknown")] += 1
            for item in event.get("abnormal_types", []):
                type_counter[item] += 1
        return {
            "msg": "success",
            "data": {
                "total": len(events_data),
                "types": dict(type_counter),
                "levels": dict(level_counter),
                "latest": events_data[-1] if events_data else None,
            },
        }

    @app.get("/api/sensors")
    def sensors(monitor_id: str = None, limit: int = 180):
        return {"msg": "success", "data": _sensor_snapshot(monitor_id, limit)}

    @app.get("/api/proxy-image")
    def proxy_image(url: str):
        try:
            resp = requests.get(url, timeout=8)
            resp.raise_for_status()
            media_type = resp.headers.get("content-type") or "image/jpeg"
            return Response(content=resp.content, media_type=media_type)
        except Exception as exc:
            return Response(content=("image proxy failed: %s" % exc).encode("utf-8"), status_code=502)

    @app.get("/api/camera-stream")
    def camera_stream(monitor_id: str = None):
        source = _camera_source(monitor_id)
        return StreamingResponse(
            _mjpeg_frames(source, monitor_id),
            media_type="multipart/x-mixed-replace; boundary=frame",
        )

    return app


def _camera_source(monitor_id):
    task = _TASKS.get(monitor_id or "")
    if task:
        return task.get("camera_source", "0")
    return _config_value("VISUALIZATION", "CAMERA_URL", os.getenv("VISUALIZATION_CAMERA_URL", "0"))


def _mjpeg_frames(source, monitor_id=None):
    capture = None
    try:
        while True:
            payload = _latest_cached_frame(monitor_id)
            if payload:
                yield b"--frame\r\nContent-Type: image/jpeg\r\nCache-Control: no-cache\r\n\r\n" + payload + b"\r\n"
                time.sleep(0.12)
                continue

            if capture is None:
                capture = _open_capture(source)
            ok, frame = capture.read()
            if not ok or frame is None:
                time.sleep(0.2)
                if not capture.isOpened():
                    capture.release()
                    capture = _open_capture(source)
                continue
            ok, encoded = cv2.imencode(".jpg", frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
            if not ok:
                continue
            payload = encoded.tobytes()
            yield b"--frame\r\nContent-Type: image/jpeg\r\nCache-Control: no-cache\r\n\r\n" + payload + b"\r\n"
    finally:
        if capture is not None:
            capture.release()


def _latest_cached_frame(monitor_id):
    if not monitor_id:
        return None
    conn = None
    try:
        import functions

        conn = functions.getRedisConn()
        payload = conn.get("monitor:%s:latest_frame_jpeg" % monitor_id)
        if payload:
            return bytes(payload)
    except Exception:
        return None
    finally:
        if conn is not None:
            try:
                functions.releaseRedisConn(conn)
            except Exception:
                pass
    return None


def _open_capture(source):
    parsed = _parse_camera_source(source)
    if isinstance(parsed, int):
        capture = cv2.VideoCapture(parsed, cv2.CAP_DSHOW)
        if not capture.isOpened():
            capture = cv2.VideoCapture(parsed)
        return capture

    os.environ.setdefault("OPENCV_FFMPEG_CAPTURE_OPTIONS", "rtsp_transport;tcp|stimeout;5000000")
    try:
        return cv2.VideoCapture(str(parsed), cv2.CAP_FFMPEG)
    except Exception:
        return cv2.VideoCapture(str(parsed))


def _parse_camera_source(source):
    text = str(source or "0").strip()
    lowered = text.lower()
    for prefix in ("camera://", "webcam://", "local_camera://"):
        if lowered.startswith(prefix):
            return int(lowered.replace(prefix, "") or "0")
    if lowered in ("camera", "webcam", "local_camera"):
        return 0
    if text.isdigit():
        return int(text)
    return text


def _consume_topic(task, topic, sink, label):
    consumer = None
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=_bootstrap_servers(),
            group_id="%s-%s-%s" % (DEFAULT_GROUP, label, task["monitor_id"]),
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            api_version_auto_timeout_ms=5000,
        )
        for record in consumer:
            if task["stop_event"].is_set():
                break
            msg = record.value
            if msg.get("monitor_id") == task["monitor_id"]:
                sink.append(msg)
    except Exception as exc:
        print("visualization %s consumer failed: %s" % (label, exc))
    finally:
        if consumer:
            consumer.close()


def _sensor_snapshot(monitor_id, limit):
    rows = _query_sensor_rows(monitor_id, limit)
    latest_by_code = {}
    for row in rows:
        latest_by_code.setdefault(row["sensor_code"], row)
    latest = list(latest_by_code.values())
    latest.sort(key=lambda item: item.get("sensor_code") or "")

    series = {}
    for row in reversed(rows):
        code = row["sensor_code"]
        series.setdefault(code, {"name": row.get("sensor_name") or code, "unit": row.get("unit") or "", "values": []})
        series[code]["values"].append({"time": _dt_text(row.get("collected_at")), "value": row.get("value")})

    return {
        "latest": latest,
        "series": series,
        "updated_at": _dt_text(max([row.get("collected_at") for row in latest], default=None)),
    }


def _query_sensor_rows(monitor_id, limit):
    conn = _mysql_conn()
    if not conn:
        return []
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            if monitor_id:
                cur.execute("""
                    SELECT monitor_id, panel_no, sensor_code, sensor_name, address, raw_value,
                           value, unit, status, collected_at
                    FROM sensor_data_record
                    WHERE monitor_id=%s
                    ORDER BY collected_at DESC, id DESC
                    LIMIT %s
                """, (monitor_id, int(limit)))
            else:
                cur.execute("""
                    SELECT monitor_id, panel_no, sensor_code, sensor_name, address, raw_value,
                           value, unit, status, collected_at
                    FROM sensor_data_record
                    ORDER BY collected_at DESC, id DESC
                    LIMIT %s
                """, (int(limit),))
            return list(cur.fetchall())
    except Exception as exc:
        print("query sensor rows failed: %s" % exc)
        return []
    finally:
        conn.close()


def _mysql_conn():
    cfg = _config_value("MYSQL", None, {}) or {}
    if not isinstance(cfg, dict):
        cfg = {}
    host = cfg.get("HOST", os.getenv("MYSQL_HOST", "127.0.0.1"))
    port = int(cfg.get("PORT", os.getenv("MYSQL_PORT", 3306)))
    user = cfg.get("USER", os.getenv("MYSQL_USER", "root"))
    password = cfg.get("PASSWORD", os.getenv("MYSQL_PASSWORD", "123456"))
    database = cfg.get("DATABASE", os.getenv("MYSQL_DATABASE", "monitoring"))
    try:
        _ensure_database(host, port, user, password, database)
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            autocommit=True,
        )
    except Exception as exc:
        print("mysql unavailable for visualization: %s" % exc)
        return None


def _ensure_database(host, port, user, password, database):
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4" % database.replace("`", ""))
            cur.execute("USE `%s`" % database.replace("`", ""))
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_data_record (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    monitor_id VARCHAR(128),
                    panel_no VARCHAR(32),
                    sensor_code VARCHAR(128),
                    sensor_name VARCHAR(128),
                    address VARCHAR(32),
                    raw_value DOUBLE,
                    value DOUBLE,
                    unit VARCHAR(32),
                    status VARCHAR(32),
                    collected_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sensor_collected_at (sensor_code, collected_at),
                    INDEX idx_monitor_collected_at (monitor_id, collected_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
    finally:
        conn.close()


def _recent(items, monitor_id, limit):
    data = [item for item in list(items) if _match_monitor(item, monitor_id)]
    return list(reversed(data[-max(1, min(int(limit), 500)):]))


def _latest(items, monitor_id):
    for item in reversed(list(items)):
        if _match_monitor(item, monitor_id):
            return item
    return None


def _match_monitor(item, monitor_id):
    return not monitor_id or item.get("monitor_id") == monitor_id


def _bootstrap_servers():
    servers = _config_value("KAFKA", "BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    if isinstance(servers, str):
        return [item.strip() for item in servers.split(",") if item.strip()]
    return servers


def _set_status(monitor_id, status):
    try:
        import functions

        conn = functions.getRedisConn()
        conn.set("monitor:%s:visualization_status" % monitor_id, status, ex=86400)
        functions.releaseRedisConn(conn)
    except Exception:
        pass


def _config_value(section, key, default=None):
    try:
        import server

        cfg = getattr(server, "config", None) or {}
    except Exception:
        cfg = {}
    section_cfg = cfg.get(section, {}) if isinstance(cfg, dict) else {}
    if key is None:
        return section_cfg if isinstance(section_cfg, dict) else default
    if isinstance(section_cfg, dict) and section_cfg.get(key) is not None:
        return section_cfg.get(key)
    return cfg.get(key, default) if isinstance(cfg, dict) else default


def _response(monitor_id, dashboard_url, status):
    base_url = dashboard_url.split("/workshop-dashboard", 1)[0]
    return {
        "msg": "success",
        "data": {
            "monitor_id": monitor_id,
            "dashboard_url": dashboard_url,
            "camera_stream_url": "%s/api/camera-stream?monitor_id=%s" % (base_url, monitor_id),
            "status": status,
        },
    }


def _record_monitor_id(record):
    if isinstance(record, dict):
        return record.get("monitor_id")
    return None


def _now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _dt_text(value):
    if not value:
        return ""
    if isinstance(value, dt.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)[:19]


HTML = r"""
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>车间监测实时可视化</title>
  <style>
    :root {
      --base: #111;
      --surface: #1c1c1c;
      --elevated: #252525;
      --border: #2a2a2a;
      --heading: #f1f5f9;
      --body: #94a3b8;
      --muted: #64748b;
      --accent: #10b981;
      --danger: #ef4444;
      --warn: #f59e0b;
      --blue: #60a5fa;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background:
        radial-gradient(circle at 18px 18px, rgba(148, 163, 184, 0.14) 1px, transparent 1px),
        linear-gradient(180deg, #111 0%, #171717 100%);
      background-size: 36px 36px, auto;
      color: var(--body);
      font-family: "Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
    }
    .shell { width: min(1500px, calc(100vw - 32px)); margin: 0 auto; padding: 22px 0; }
    .topbar, .panel, .metric {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
    }
    .topbar { min-height: 84px; display: flex; align-items: center; justify-content: space-between; padding: 18px 22px; margin-bottom: 16px; }
    .eyebrow { color: var(--accent); font-size: 11px; font-weight: 700; letter-spacing: .08em; margin: 0 0 8px; }
    h1, h2 { color: var(--heading); margin: 0; letter-spacing: 0; }
    h1 { font-size: 24px; }
    h2 { font-size: 16px; }
    .toolbar { display: flex; gap: 10px; align-items: center; white-space: nowrap; }
    .pill { border: 1px solid var(--border); background: var(--elevated); border-radius: 999px; padding: 7px 10px; font-size: 12px; }
    .status-dot { width: 9px; height: 9px; border-radius: 999px; display: inline-block; background: var(--muted); }
    .status-dot.online { background: var(--accent); box-shadow: 0 0 0 4px rgba(16,185,129,.12); }
    .status-dot.error { background: var(--danger); box-shadow: 0 0 0 4px rgba(239,68,68,.12); }
    .layout { display: grid; grid-template-columns: 1.45fr .9fr; gap: 16px; }
    .panel { padding: 18px; min-width: 0; }
    .panel-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
    .camera-wrap { position: relative; aspect-ratio: 16 / 9; background: #070707; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
    .camera-wrap img { width: 100%; height: 100%; object-fit: contain; display: block; }
    .empty { color: var(--muted); display: grid; min-height: 100%; place-items: center; }
    .region { position: absolute; border: 3px solid var(--danger); box-shadow: 0 0 0 1px rgba(255,255,255,.25), 0 0 18px rgba(239,68,68,.55); pointer-events: none; }
    .region span { position: absolute; left: -3px; top: -28px; background: var(--danger); color: #fff; font-size: 12px; padding: 4px 8px; border-radius: 6px 6px 6px 0; white-space: nowrap; }
    .metrics { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; margin-bottom: 16px; }
    .metric { min-height: 102px; padding: 16px; position: relative; }
    .metric strong { color: var(--heading); display: block; font: 700 28px Consolas, monospace; margin-top: 18px; }
    .metric small, .caption { color: var(--muted); font-size: 12px; }
    .metric.warn { border-color: rgba(245,158,11,.55); }
    .metric.danger { border-color: rgba(239,68,68,.7); }
    .split { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }
    .list { display: grid; gap: 10px; max-height: 420px; overflow: auto; }
    .item { background: var(--elevated); border: 1px solid var(--border); border-left: 4px solid var(--danger); border-radius: 8px; padding: 11px 12px; }
    .item strong { color: var(--heading); display: block; font-size: 14px; margin-bottom: 6px; }
    .item p { margin: 0; font-size: 13px; line-height: 1.5; }
    .table-wrap { max-height: 360px; overflow: auto; }
    table { border-collapse: collapse; width: 100%; min-width: 720px; }
    th, td { border-bottom: 1px solid var(--border); padding: 10px; text-align: left; font-size: 13px; }
    th { color: var(--muted); font-weight: 700; }
    td { color: var(--body); }
    .charts { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 16px; }
    canvas { background: #151515; border: 1px solid var(--border); border-radius: 8px; width: 100%; height: 260px; display: block; }
    a { color: var(--blue); word-break: break-all; }
    @media (max-width: 1050px) { .layout, .split, .charts { grid-template-columns: 1fr; } .metrics { grid-template-columns: repeat(2, 1fr); } }
    @media (max-width: 640px) { .topbar { align-items: flex-start; flex-direction: column; } .metrics { grid-template-columns: 1fr; } }
  </style>
</head>
<body>
  <main class="shell">
    <header class="topbar">
      <div>
        <p class="eyebrow">WORKSHOP MONITORING</p>
        <h1>车间监测实时可视化面板</h1>
      </div>
      <div class="toolbar">
        <span id="statusDot" class="status-dot"></span>
        <span id="statusText" class="pill">连接中</span>
        <span id="monitorLabel" class="pill">--</span>
      </div>
    </header>

    <section class="metrics">
      <article class="metric"><small>异常总数</small><strong id="totalEvents">0</strong></article>
      <article class="metric"><small>当前人数</small><strong id="personCount">0</strong></article>
      <article class="metric"><small>最新等级</small><strong id="latestLevel">--</strong></article>
      <article class="metric"><small>更新时间</small><strong id="updatedAt" style="font-size:18px">--</strong></article>
    </section>

    <section class="layout">
      <article class="panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">LIVE CAMERA</p>
            <h2>实时摄像头画面与异常红框</h2>
          </div>
          <span id="clipInfo" class="caption">等待视频帧</span>
        </div>
        <div id="camera" class="camera-wrap"><div class="empty">等待识别画面</div></div>
      </article>

      <article class="panel">
        <div class="panel-head">
          <div>
            <p class="eyebrow">EVENTS</p>
            <h2>异常事件列表</h2>
          </div>
        </div>
        <div id="events" class="list"><span class="caption">暂无事件</span></div>
      </article>
    </section>

    <section class="split">
      <article class="panel">
        <div class="panel-head"><div><p class="eyebrow">SENSORS</p><h2>实时传感器数据</h2></div><span id="sensorTime" class="caption">--</span></div>
        <div class="table-wrap">
          <table>
            <thead><tr><th>传感器</th><th>地址</th><th>当前值</th><th>单位</th><th>状态</th><th>采集时间</th></tr></thead>
            <tbody id="sensorRows"><tr><td colspan="6" class="caption">等待传感器数据</td></tr></tbody>
          </table>
        </div>
      </article>

      <article class="panel">
        <div class="panel-head"><div><p class="eyebrow">ALARMS</p><h2>报警推送状态</h2></div></div>
        <div id="alarms" class="list"><span class="caption">暂无报警</span></div>
      </article>
    </section>

    <section class="charts">
      <article class="panel">
        <div class="panel-head"><div><p class="eyebrow">TYPE DISTRIBUTION</p><h2>异常类型分布</h2></div></div>
        <canvas id="typeChart"></canvas>
      </article>
      <article class="panel">
        <div class="panel-head"><div><p class="eyebrow">SENSOR TREND</p><h2>传感器趋势</h2></div></div>
        <canvas id="sensorChart"></canvas>
      </article>
    </section>
  </main>

  <script>
    const params = new URLSearchParams(location.search);
    const monitorId = params.get("monitor_id") || "";
    document.getElementById("monitorLabel").textContent = monitorId || "全部监控";

    function q(path) {
      const join = path.includes("?") ? "&" : "?";
      return monitorId ? `${path}${join}monitor_id=${encodeURIComponent(monitorId)}` : path;
    }
    async function json(path) {
      const res = await fetch(q(path), { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const body = await res.json();
      return body.data || body;
    }
    function setStatus(type, text) {
      document.getElementById("statusDot").className = `status-dot ${type}`;
      document.getElementById("statusText").textContent = text;
    }
    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, s => ({ "&":"&amp;", "<":"&lt;", ">":"&gt;", '"':"&quot;", "'":"&#39;" }[s]));
    }
    function num(value, digits = 2) {
      if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
      const n = Number(value);
      return Number.isInteger(n) ? String(n) : n.toFixed(digits);
    }
    function latestRegions(live) {
      return live.event?.abnormal_regions || [];
    }
    function renderCamera(live) {
      const regions = latestRegions(live);
      const camera = document.getElementById("camera");
      const effectiveMonitorId = monitorId || live.monitor_id || live.event?.monitor_id || live.recognition?.monitor_id || "";
      const streamUrl = effectiveMonitorId
        ? `/api/camera-stream?monitor_id=${encodeURIComponent(effectiveMonitorId)}`
        : "/api/camera-stream";
      if (camera.dataset.streamUrl !== streamUrl) {
        camera.dataset.streamUrl = streamUrl;
        camera.innerHTML = `<img id="cameraImg" src="${esc(streamUrl)}" alt="实时摄像头画面">`;
        const img = document.getElementById("cameraImg");
        img.addEventListener("load", () => drawRegions(camera, img, regions));
      }
      const img = document.getElementById("cameraImg");
      if (img && img.complete) {
        drawRegions(camera, img, regions);
      }
      document.getElementById("clipInfo").textContent = live.event?.clip_id || live.recognition?.clip_id || "实时视频流";
    }
    function drawRegions(container, img, regions) {
      container.querySelectorAll(".region").forEach(item => item.remove());
      const naturalW = img.naturalWidth || 1;
      const naturalH = img.naturalHeight || 1;
      const box = img.getBoundingClientRect();
      const parent = container.getBoundingClientRect();
      const scale = Math.min(box.width / naturalW, box.height / naturalH);
      const displayW = naturalW * scale;
      const displayH = naturalH * scale;
      const offsetX = (parent.width - displayW) / 2;
      const offsetY = (parent.height - displayH) / 2;
      regions.forEach(region => {
        const b = region.bbox || [];
        if (b.length !== 4) return;
        const el = document.createElement("div");
        el.className = "region";
        el.style.left = `${offsetX + b[0] * scale}px`;
        el.style.top = `${offsetY + b[1] * scale}px`;
        el.style.width = `${Math.max(2, (b[2] - b[0]) * scale)}px`;
        el.style.height = `${Math.max(2, (b[3] - b[1]) * scale)}px`;
        el.innerHTML = `<span>${esc(region.label || region.type || "异常")}</span>`;
        container.appendChild(el);
      });
    }
    function renderEvents(events) {
      document.getElementById("events").innerHTML = events.map(e => `
        <div class="item">
          <strong>${esc(e.event_time || "")} ${esc((e.abnormal_types || []).join(", "))}</strong>
          <p>${esc(e.event_description || "")}</p>
          ${e.evidence_video_url ? `<p><a href="${esc(e.evidence_video_url)}" target="_blank">证据视频</a></p>` : ""}
        </div>
      `).join("") || '<span class="caption">暂无事件</span>';
    }
    function renderAlarms(alarms) {
      document.getElementById("alarms").innerHTML = alarms.map(a => `
        <div class="item">
          <strong>${esc(a.alarm_time || "")} ${esc(a.notify_status || "")}</strong>
          <p>${esc(a.notify_message || a.notify_status || "")}</p>
        </div>
      `).join("") || '<span class="caption">暂无报警</span>';
    }
    function renderSensors(data) {
      document.getElementById("sensorTime").textContent = data.updated_at || "--";
      const rows = data.latest || [];
      document.getElementById("sensorRows").innerHTML = rows.map(row => `
        <tr>
          <td>${esc(row.sensor_name || row.sensor_code)}</td>
          <td>${esc(row.address)}</td>
          <td>${num(row.value)}</td>
          <td>${esc(row.unit || "")}</td>
          <td>${esc(row.status || "")}</td>
          <td>${esc((row.collected_at || "").slice(0, 19))}</td>
        </tr>
      `).join("") || '<tr><td colspan="6" class="caption">等待传感器数据</td></tr>';
      drawSensorChart(data.series || {});
    }
    function setupCanvas(id) {
      const canvas = document.getElementById(id);
      const rect = canvas.getBoundingClientRect();
      const ratio = window.devicePixelRatio || 1;
      canvas.width = Math.max(1, Math.floor(rect.width * ratio));
      canvas.height = Math.max(1, Math.floor(rect.height * ratio));
      const ctx = canvas.getContext("2d");
      ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
      return { ctx, w: rect.width, h: rect.height };
    }
    function drawTypeChart(types) {
      const { ctx, w, h } = setupCanvas("typeChart");
      ctx.clearRect(0, 0, w, h);
      const entries = Object.entries(types || {});
      if (!entries.length) return emptyChart(ctx, w, h, "暂无异常事件");
      const max = Math.max(...entries.map(([,v]) => v), 1);
      const pad = 36, gap = 12;
      const bw = Math.max(18, (w - pad * 2 - gap * (entries.length - 1)) / entries.length);
      entries.forEach(([name, value], i) => {
        const x = pad + i * (bw + gap);
        const bh = (h - 80) * value / max;
        const y = h - 42 - bh;
        ctx.fillStyle = "#ef4444";
        ctx.fillRect(x, y, bw, bh);
        ctx.fillStyle = "#94a3b8";
        ctx.font = "12px Consolas, monospace";
        ctx.fillText(value, x, y - 8);
        ctx.save();
        ctx.translate(x, h - 24);
        ctx.rotate(-0.35);
        ctx.fillText(name, 0, 0);
        ctx.restore();
      });
    }
    function drawSensorChart(series) {
      const { ctx, w, h } = setupCanvas("sensorChart");
      ctx.clearRect(0, 0, w, h);
      const colors = {
        voltage: "#10b981",
        temperature: "#60a5fa",
        humidity: "#a78bfa",
        noise: "#fbbf24",
        current: "#fb7185",
        frequency: "#22d3ee",
      };
      const preferred = ["voltage", "temperature", "humidity", "noise", "current", "frequency"];
      const items = preferred
        .filter(code => series[code]?.values?.length)
        .map(code => ({ code, ...series[code], color: colors[code] || "#10b981" }));
      if (!items.length) return emptyChart(ctx, w, h, "暂无传感器趋势");
      const pad = { l: 58, r: 22, t: 38, b: 38 };
      const plotW = w - pad.l - pad.r;
      const plotH = h - pad.t - pad.b;
      ctx.strokeStyle = "#2a2a2a";
      ctx.beginPath();
      for (let i = 0; i <= 4; i++) {
        const y = pad.t + plotH * i / 4;
        ctx.moveTo(pad.l, y); ctx.lineTo(w - pad.r, y);
      }
      ctx.stroke();

      ctx.fillStyle = "#64748b";
      ctx.font = "12px Consolas, monospace";
      ctx.textAlign = "right";
      for (let i = 0; i <= 4; i++) {
        const value = 100 - i * 25;
        const y = pad.t + plotH * i / 4;
        ctx.fillText(value + "%", pad.l - 10, y + 4);
      }
      ctx.textAlign = "left";

      items.forEach(item => {
        const values = (item.values || []).filter(point => Number.isFinite(Number(point.value)));
        if (!values.length) return;
        let min = Math.min(...values.map(point => Number(point.value)));
        let max = Math.max(...values.map(point => Number(point.value)));
        if (min === max) { min -= 1; max += 1; }
        ctx.strokeStyle = item.color;
        ctx.lineWidth = 2;
        ctx.beginPath();
        values.forEach((point, i) => {
          const x = values.length === 1 ? pad.l : pad.l + plotW * i / (values.length - 1);
          const y = pad.t + plotH - ((Number(point.value) - min) / (max - min)) * plotH;
          if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
        });
        ctx.stroke();
        values.forEach((point, i) => {
          if (values.length > 1 && i % Math.max(1, Math.floor(values.length / 12)) !== 0 && i !== values.length - 1) return;
          const x = values.length === 1 ? pad.l : pad.l + plotW * i / (values.length - 1);
          const y = pad.t + plotH - ((Number(point.value) - min) / (max - min)) * plotH;
          ctx.fillStyle = item.color;
          ctx.beginPath();
          ctx.arc(x, y, 3, 0, Math.PI * 2);
          ctx.fill();
        });
      });

      let lx = pad.l;
      items.forEach(item => {
        const label = `${item.name || item.code}${item.unit ? " " + item.unit : ""}`;
        ctx.fillStyle = item.color;
        ctx.fillRect(lx, 14, 20, 3);
        ctx.fillStyle = "#94a3b8";
        ctx.font = "12px Microsoft YaHei, sans-serif";
        ctx.fillText(label, lx + 28, 18);
        lx += Math.min(150, 42 + label.length * 12);
      });
    }
    function emptyChart(ctx, w, h, text) {
      ctx.fillStyle = "#64748b";
      ctx.font = "14px Microsoft YaHei, sans-serif";
      ctx.fillText(text, 20, 34);
    }
    async function refresh() {
      try {
        const [live, eventsBody, alarmsBody, statsBody, sensors] = await Promise.all([
          json("/api/live"),
          json("/api/events?limit=40"),
          json("/api/alarms?limit=40"),
          json("/api/stats"),
          json("/api/sensors?limit=240"),
        ]);
        renderCamera(live);
        renderEvents(eventsBody);
        renderAlarms(alarmsBody);
        renderSensors(sensors);
        document.getElementById("totalEvents").textContent = statsBody.total || 0;
        document.getElementById("personCount").textContent = live.recognition?.scene_result?.person_count ?? 0;
        document.getElementById("latestLevel").textContent = live.event?.abnormal_level || "--";
        document.getElementById("updatedAt").textContent = live.updated_at || "--";
        drawTypeChart(statsBody.types || {});
        setStatus("online", "实时刷新中");
      } catch (err) {
        setStatus("error", err.message);
      }
    }
    refresh();
    setInterval(refresh, 2500);
    window.addEventListener("resize", refresh);
  </script>
</body>
</html>
"""
