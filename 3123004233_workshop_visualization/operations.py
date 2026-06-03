#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import threading
from collections import Counter, deque

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from kafka import KafkaConsumer


ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
ALARM_RESULT_TOPIC = "workshop.alarm_result"
DEFAULT_GROUP = "workshop-visualization"

_TASKS = {}
_LOCK = threading.Lock()
_EVENTS = deque(maxlen=200)
_ALARMS = deque(maxlen=200)
_SERVER_STARTED = False


def start_visualize(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    abnormal_topic = (data or {}).get("abnormal_event_topic") or _config_value("KAFKA", "ABNORMAL_EVENT_TOPIC", ABNORMAL_EVENT_TOPIC)
    alarm_topic = (data or {}).get("alarm_result_topic") or _config_value("KAFKA", "ALARM_RESULT_TOPIC", ALARM_RESULT_TOPIC)
    port = int(_config_value("VISUALIZATION", "PORT", os.getenv("VISUALIZATION_PORT", 8088)))
    host = _config_value("VISUALIZATION", "HOST", os.getenv("VISUALIZATION_HOST", "127.0.0.1"))
    dashboard_url = "http://%s:%s/workshop-dashboard?monitor_id=%s" % (host, port, monitor_id)

    with _LOCK:
        task = _TASKS.get(monitor_id)
        if task and task.get("status") == "running":
            return _response(monitor_id, dashboard_url, "running")

        task = {
            "monitor_id": monitor_id,
            "abnormal_topic": abnormal_topic,
            "alarm_topic": alarm_topic,
            "status": "running",
            "stop_event": threading.Event(),
        }
        _TASKS[monitor_id] = task
        threading.Thread(target=_consume_events, args=(task,), daemon=True, name="visual-events-%s" % monitor_id).start()
        threading.Thread(target=_consume_alarms, args=(task,), daemon=True, name="visual-alarms-%s" % monitor_id).start()
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

    @app.get("/api/events")
    def events(monitor_id: str = None):
        data = [item for item in list(_EVENTS) if not monitor_id or item.get("monitor_id") == monitor_id]
        return list(reversed(data[-100:]))

    @app.get("/api/alarms")
    def alarms(monitor_id: str = None):
        data = [item for item in list(_ALARMS) if not monitor_id or item.get("monitor_id") == monitor_id]
        return list(reversed(data[-100:]))

    @app.get("/api/stats")
    def stats(monitor_id: str = None):
        data = [item for item in list(_EVENTS) if not monitor_id or item.get("monitor_id") == monitor_id]
        type_counter = Counter()
        level_counter = Counter()
        for event in data:
            level_counter[event.get("abnormal_level", "unknown")] += 1
            for item in event.get("abnormal_types", []):
                type_counter[item] += 1
        return {
            "total": len(data),
            "types": dict(type_counter),
            "levels": dict(level_counter),
            "latest": data[-1] if data else None,
        }

    return app


def _consume_events(task):
    _consume_topic(task, task["abnormal_topic"], _EVENTS, "event")


def _consume_alarms(task):
    _consume_topic(task, task["alarm_topic"], _ALARMS, "alarm")


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
    if isinstance(section_cfg, dict) and section_cfg.get(key) is not None:
        return section_cfg.get(key)
    return cfg.get(key, default) if isinstance(cfg, dict) else default


def _response(monitor_id, dashboard_url, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "dashboard_url": dashboard_url, "status": status}}


HTML = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>车间视频监测</title>
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    body { margin:0; font-family: Arial, "Microsoft YaHei", sans-serif; background:#f4f6f8; color:#17202a; }
    header { height:56px; display:flex; align-items:center; padding:0 24px; background:#152238; color:#fff; font-size:20px; font-weight:700; }
    main { padding:18px; display:grid; grid-template-columns: 1.2fr .8fr; gap:16px; }
    section { background:#fff; border:1px solid #d8dee6; border-radius:6px; padding:14px; min-width:0; }
    h2 { font-size:15px; margin:0 0 12px; }
    .media { aspect-ratio:16/9; background:#101820; display:flex; align-items:center; justify-content:center; overflow:hidden; border-radius:4px; }
    .media img { width:100%; height:100%; object-fit:contain; }
    .grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .list { display:flex; flex-direction:column; gap:10px; max-height:460px; overflow:auto; }
    .item { border-left:4px solid #d7263d; background:#f8fafc; padding:10px; border-radius:4px; font-size:13px; }
    .muted { color:#687385; }
    .charts { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
    .chart { height:260px; }
    a { color:#0b6bcb; word-break:break-all; }
    @media (max-width: 900px) { main, .grid, .charts { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <header>车间视频监测异常行为分析系统</header>
  <main>
    <section>
      <h2>实时异常画面</h2>
      <div class="media" id="media"><span class="muted">等待异常证据帧</span></div>
    </section>
    <section>
      <h2>异常概览</h2>
      <div id="summary" class="muted">加载中</div>
    </section>
    <section>
      <h2>异常事件</h2>
      <div id="events" class="list"></div>
    </section>
    <section>
      <h2>报警状态</h2>
      <div id="alarms" class="list"></div>
    </section>
    <section style="grid-column:1 / -1">
      <h2>统计图表</h2>
      <div class="charts">
        <div id="typeChart" class="chart"></div>
        <div id="levelChart" class="chart"></div>
      </div>
    </section>
  </main>
  <script>
    const params = new URLSearchParams(location.search);
    const monitorId = params.get("monitor_id") || "";
    const typeChart = echarts.init(document.getElementById("typeChart"));
    const levelChart = echarts.init(document.getElementById("levelChart"));
    async function loadJson(path) {
      const q = monitorId ? `?monitor_id=${encodeURIComponent(monitorId)}` : "";
      const res = await fetch(path + q);
      return res.json();
    }
    function item(text, sub) {
      return `<div class="item"><div>${text}</div><div class="muted">${sub || ""}</div></div>`;
    }
    async function refresh() {
      const [events, alarms, stats] = await Promise.all([loadJson("/api/events"), loadJson("/api/alarms"), loadJson("/api/stats")]);
      const latest = stats.latest;
      document.getElementById("summary").innerHTML = `异常总数：<b>${stats.total}</b><br>当前任务：${monitorId || "全部"}`;
      document.getElementById("events").innerHTML = events.map(e => item(`${e.event_time || ""} ${e.abnormal_level || ""} ${e.event_description || ""}`, `<a href="${e.evidence_video_url || "#"}" target="_blank">证据视频</a>`)).join("") || "<span class='muted'>暂无事件</span>";
      document.getElementById("alarms").innerHTML = alarms.map(a => item(`${a.alarm_time || ""} ${a.notify_status || ""}`, a.notify_message || "")).join("") || "<span class='muted'>暂无报警</span>";
      if (latest && latest.evidence_frame_url) {
        document.getElementById("media").innerHTML = `<img src="${latest.evidence_frame_url}" alt="异常证据帧">`;
      }
      typeChart.setOption({ tooltip:{}, xAxis:{type:"category", data:Object.keys(stats.types)}, yAxis:{type:"value"}, series:[{type:"bar", data:Object.values(stats.types), color:"#0b6bcb"}] });
      levelChart.setOption({ tooltip:{}, series:[{type:"pie", radius:"65%", data:Object.entries(stats.levels).map(([name,value]) => ({name, value}))}] });
    }
    refresh();
    setInterval(refresh, 3000);
  </script>
</body>
</html>
"""
