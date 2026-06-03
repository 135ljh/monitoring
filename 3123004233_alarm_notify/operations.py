#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import threading

import requests
from kafka import KafkaConsumer, KafkaProducer


ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
ALARM_RESULT_TOPIC = "workshop.alarm_result"
DEFAULT_GROUP = "workshop-alarm-notify"

_TASKS = {}
_LOCK = threading.Lock()


def start_notify(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    input_topic = (data or {}).get("abnormal_event_topic") or _config_value("KAFKA", "ABNORMAL_EVENT_TOPIC", ABNORMAL_EVENT_TOPIC)
    output_topic = _config_value("KAFKA", "ALARM_RESULT_TOPIC", ALARM_RESULT_TOPIC)

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
        thread = threading.Thread(target=_consume_loop, args=(task,), daemon=True, name="alarm-notify-%s" % monitor_id)
        task["thread"] = thread
        thread.start()

    _set_status(monitor_id, "running")
    return _response(monitor_id, output_topic, "running")


def _consume_loop(task):
    consumer = None
    producer = None
    try:
        consumer = _create_consumer(task["input_topic"], DEFAULT_GROUP + "-" + task["monitor_id"])
        producer = _create_producer()
        for record in consumer:
            if task["stop_event"].is_set():
                break
            event = record.value
            if event.get("monitor_id") != task["monitor_id"]:
                continue
            result = _handle_event(event)
            producer.send(task["output_topic"], result)
            producer.flush(timeout=10)
            print("published alarm result: %s" % json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        print("alarm notify task failed: %s" % exc)
        _update_task(task["monitor_id"], status="error", last_error=str(exc))
        _set_status(task["monitor_id"], "error")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()


def _handle_event(event):
    cooldown_key = "monitor:%s:alarm:%s:cooldown" % (
        event.get("monitor_id"),
        "_".join(event.get("abnormal_types", ["unknown"])),
    )
    if _in_cooldown(cooldown_key):
        return _alarm_result(event, False, [], "cooldown", "skipped", "skipped")

    methods = []
    sound_status = _call_sound_light(event)
    if sound_status != "not_configured":
        methods.append("sound_light")

    notify_status = _call_enterprise_notify(event)
    if notify_status != "not_configured":
        methods.append(_config_value("ALARM", "NOTIFY_METHOD", "webhook"))

    if not methods:
        methods = ["record_only"]

    cooldown_seconds = int(_config_value("ALARM", "COOLDOWN_SECONDS", 300))
    _set_key(cooldown_key, "1", cooldown_seconds)
    return _alarm_result(event, True, methods, _message(event), notify_status, sound_status)


def _call_sound_light(event):
    url = _config_value("ALARM", "SOUND_LIGHT_URL", os.getenv("SOUND_LIGHT_URL"))
    if not url:
        return "not_configured"
    try:
        resp = requests.post(url, json=event, timeout=5)
        return "success" if resp.status_code < 400 else "failed:%s" % resp.status_code
    except Exception as exc:
        return "failed:%s" % exc


def _call_enterprise_notify(event):
    url = _config_value("ALARM", "WEBHOOK_URL", os.getenv("ALARM_WEBHOOK_URL"))
    if not url:
        return "not_configured"
    payload = {"text": _message(event), "event": event}
    try:
        resp = requests.post(url, json=payload, timeout=8)
        return "success" if resp.status_code < 400 else "failed:%s" % resp.status_code
    except Exception as exc:
        return "failed:%s" % exc


def _alarm_result(event, triggered, methods, message, notify_status, sound_status):
    return {
        "monitor_id": event.get("monitor_id"),
        "event_id": event.get("event_id"),
        "alarm_triggered": triggered,
        "alarm_methods": methods,
        "notify_message": message,
        "alarm_time": _now_text(),
        "notify_status": notify_status,
        "sound_light_status": sound_status,
    }


def _message(event):
    return "车间检测到异常行为：%s。请及时处理。" % event.get("event_description", "未知异常")


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


def _in_cooldown(key):
    conn = _redis_conn()
    if not conn:
        return False
    try:
        return bool(conn.exists(key))
    finally:
        conn.close()


def _set_key(key, value, ex):
    conn = _redis_conn()
    if not conn:
        return
    try:
        conn.set(key, value, ex=ex)
    finally:
        conn.close()


def _set_status(monitor_id, status):
    _set_key("monitor:%s:alarm_status" % monitor_id, status, 86400)


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


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "alarm_result_topic": topic, "status": status}}
