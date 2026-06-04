#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import threading
import uuid

import pymysql
from kafka import KafkaConsumer, KafkaProducer


RECOGNITION_RESULT_TOPIC = "workshop.recognition_result"
ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
DEFAULT_GROUP = "workshop-abnormal-judgement"
DEFAULT_STATIC_SECONDS = 10.0

_TASKS = {}
_LOCK = threading.Lock()


def start_judge(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    input_topic = (data or {}).get("recognition_result_topic") or _config_value(
        "KAFKA", "RECOGNITION_RESULT_TOPIC", RECOGNITION_RESULT_TOPIC)
    output_topic = _config_value("KAFKA", "ABNORMAL_EVENT_TOPIC", ABNORMAL_EVENT_TOPIC)

    with _LOCK:
        task = _TASKS.get(monitor_id)
        if task and task.get("status") == "running":
            return _response(monitor_id, output_topic, "running")

        task = {
            "monitor_id": monitor_id,
            "input_topic": input_topic,
            "output_topic": output_topic,
            "status": "running",
            "stop_event": threading.Event(),
        }
        _TASKS[monitor_id] = task
        thread = threading.Thread(target=_consume_loop, args=(task,), daemon=True, name="abnormal-judge-%s" % monitor_id)
        task["thread"] = thread
        thread.start()

    _set_status(monitor_id, "running")
    return _response(monitor_id, output_topic, "running")


def _consume_loop(task):
    consumer = None
    producer = None
    try:
        _init_mysql()
        consumer = _create_consumer(task["input_topic"], DEFAULT_GROUP + "-" + task["monitor_id"])
        producer = _create_producer()
        for record in consumer:
            if task["stop_event"].is_set():
                break
            msg = record.value
            if msg.get("monitor_id") != task["monitor_id"]:
                continue
            event = _judge_message(msg)
            if not event:
                continue
            _save_event(event)
            producer.send(task["output_topic"], event)
            producer.flush(timeout=10)
            print("published abnormal event: %s" % json.dumps(event, ensure_ascii=False))
    except Exception as exc:
        print("abnormal judgement task failed: %s" % exc)
        _update_task(task["monitor_id"], status="error", last_error=str(exc))
        _set_status(task["monitor_id"], "error")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()


def _judge_message(msg):
    monitor_id = msg["monitor_id"]
    abnormal_types = []
    descriptions = []

    for person in msg.get("person_results", []):
        if person.get("action_type") != "static":
            _delete_key("monitor:%s:person:%s:static_start_time" % (monitor_id, person.get("person_id")))
            continue

        person_id = person.get("person_id", "unknown")
        duration = _accumulate_static_duration(monitor_id, person_id, msg)
        if duration >= float(_config_value("JUDGE", "STATIC_SECONDS", DEFAULT_STATIC_SECONDS)):
            abnormal_types.append("person_static")
            descriptions.append("人员%s静止超过%.1f秒" % (person_id, duration))

    for device in msg.get("device_results", []):
        key = "monitor:%s:device:%s:vibration_history" % (monitor_id, device.get("device_id", "unknown"))
        _push_history(key, device.get("vibration_score", 0.0))
        if device.get("vibration_level") == "danger":
            abnormal_types.append("device_vibration")
            descriptions.append("设备%s存在异常震动" % device.get("device_id", "unknown"))

    abnormal_types = sorted(set(abnormal_types))
    if not abnormal_types:
        return None

    level = _event_level(abnormal_types)
    return {
        "monitor_id": monitor_id,
        "event_id": "evt_%s" % uuid.uuid4().hex[:12],
        "is_abnormal": True,
        "abnormal_types": abnormal_types,
        "abnormal_level": level,
        "event_description": "，".join(descriptions) if descriptions else "检测到异常行为",
        "evidence_video_url": msg.get("annotated_video_url"),
        "evidence_frame_url": msg.get("annotated_frame_url"),
        "event_time": _now_text(),
        "camera_id": msg.get("camera_id"),
        "clip_id": msg.get("clip_id"),
    }


def _accumulate_static_duration(monitor_id, person_id, msg):
    start_key = "monitor:%s:person:%s:static_start_time" % (monitor_id, person_id)
    duration_key = "monitor:%s:person:%s:static_duration" % (monitor_id, person_id)
    seen_key = "monitor:%s:person:%s:last_seen_time" % (monitor_id, person_id)
    now = _parse_time(msg.get("end_time")) or dt.datetime.now()

    start_raw = _get_key(start_key)
    if start_raw:
        start = _parse_time(start_raw)
    else:
        start = _parse_time(msg.get("start_time")) or now
        _set_key(start_key, _format_dt(start), 86400)

    duration = max(0.0, (now - start).total_seconds())
    _set_key(duration_key, duration, 86400)
    _set_key(seen_key, _format_dt(now), 86400)
    return duration


def _event_level(types):
    if "device_vibration" in types and "person_static" in types:
        return "high"
    if "device_vibration" in types:
        return "high"
    if "person_static" in types:
        return "medium"
    return "low"


def _init_mysql():
    conn = _mysql_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS abnormal_event (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    monitor_id VARCHAR(128),
                    event_id VARCHAR(128) UNIQUE,
                    camera_id VARCHAR(128),
                    clip_id VARCHAR(128),
                    abnormal_types VARCHAR(512),
                    abnormal_level VARCHAR(32),
                    event_description TEXT,
                    evidence_video_url TEXT,
                    evidence_frame_url TEXT,
                    event_time DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
        conn.commit()
    finally:
        conn.close()


def _save_event(event):
    conn = _mysql_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO abnormal_event (
                    monitor_id, event_id, camera_id, clip_id, abnormal_types, abnormal_level,
                    event_description, evidence_video_url, evidence_frame_url, event_time
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE event_description=VALUES(event_description)
            """, (
                event.get("monitor_id"),
                event.get("event_id"),
                event.get("camera_id"),
                event.get("clip_id"),
                json.dumps(event.get("abnormal_types", []), ensure_ascii=False),
                event.get("abnormal_level"),
                event.get("event_description"),
                event.get("evidence_video_url"),
                event.get("evidence_frame_url"),
                event.get("event_time"),
            ))
        conn.commit()
    except Exception as exc:
        print("save abnormal event failed: %s" % exc)
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
            autocommit=False,
        )
    except Exception as exc:
        print("mysql unavailable, skip db write: %s" % exc)
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
    finally:
        conn.close()


def _create_consumer(topic, group_id):
    return KafkaConsumer(
        topic,
        bootstrap_servers=_bootstrap_servers(),
        group_id=group_id,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        api_version_auto_timeout_ms=5000,
    )


def _create_producer():
    return KafkaProducer(
        bootstrap_servers=_bootstrap_servers(),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        api_version_auto_timeout_ms=5000,
        retries=3,
    )


def _bootstrap_servers():
    servers = _config_value("KAFKA", "BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    if isinstance(servers, str):
        return [item.strip() for item in servers.split(",") if item.strip()]
    return servers


def _redis_conn():
    try:
        import functions

        return functions.getRedisConn()
    except Exception:
        return None


def _get_key(key):
    conn = _redis_conn()
    if not conn:
        return None
    try:
        value = conn.get(key)
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value
    finally:
        conn.close()


def _set_key(key, value, ex=None):
    conn = _redis_conn()
    if not conn:
        return
    try:
        conn.set(key, value, ex=ex)
    finally:
        conn.close()


def _delete_key(key):
    conn = _redis_conn()
    if not conn:
        return
    try:
        conn.delete(key)
    finally:
        conn.close()


def _push_history(key, value):
    conn = _redis_conn()
    if not conn:
        return
    try:
        conn.lpush(key, value)
        conn.ltrim(key, 0, 29)
        conn.expire(key, 86400)
    finally:
        conn.close()


def _set_status(monitor_id, status):
    _set_key("monitor:%s:judge_status" % monitor_id, status, 86400)


def _config_value(section, key, default=None):
    try:
        import server

        cfg = getattr(server, "config", None) or {}
    except Exception:
        cfg = {}
    if key is None:
        return cfg.get(section, default) if isinstance(cfg, dict) else default
    section_cfg = cfg.get(section, {}) if isinstance(cfg, dict) else {}
    if isinstance(section_cfg, dict) and section_cfg.get(key) is not None:
        return section_cfg.get(key)
    return cfg.get(key, default) if isinstance(cfg, dict) else default


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _parse_time(value):
    if not value:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(str(value)[:19], fmt)
        except ValueError:
            pass
    return None


def _now_text():
    return _format_dt(dt.datetime.now())


def _format_dt(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "abnormal_event_topic": topic, "status": status}}
