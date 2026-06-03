#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import socket
import threading
import time

import pyttsx3
import requests
from kafka import KafkaConsumer, KafkaProducer
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException


ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
ALARM_RESULT_TOPIC = "workshop.alarm_result"
DEFAULT_GROUP = "workshop-alarm-notify"

DEFAULT_PLC_IP = "10.21.2.233"
DEFAULT_PLC_PORT = 502
DEFAULT_PLC_TIMEOUT = 3
DEFAULT_PLC_DEVICE_ID = 1
DEFAULT_ALARM_ENABLE_ADDRESS = "D20"
DEFAULT_BUZZER_ADDRESS = "D2503"
DEFAULT_BUZZER_SECONDS = 1.0

DEFAULT_TTS_HOST = "10.21.2.224"
DEFAULT_TTS_PORT = 50000
DEFAULT_TTS_TIMEOUT = 3.0
DEFAULT_TTS_RATE = 150
DEFAULT_TTS_VOLUME = 0.9

_TASKS = {}
_LOCK = threading.Lock()


def start_notify(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    input_topic = (data or {}).get("abnormal_event_topic") or _config_value(
        "KAFKA", "ABNORMAL_EVENT_TOPIC", ABNORMAL_EVENT_TOPIC)
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
        return _alarm_result(event, False, [], "cooldown", "skipped", "skipped", "skipped")

    methods = []

    sound_status = _call_sound_light(event)
    if not sound_status.startswith("not_configured"):
        methods.append("sound_light")

    voice_status = _call_voice_alarm(event)
    if not voice_status.startswith("not_configured"):
        methods.append("voice")

    notify_status = _call_enterprise_notify(event)
    if notify_status != "not_configured":
        methods.append(_config_value("ALARM", "NOTIFY_METHOD", "webhook"))

    if not methods:
        methods = ["record_only"]

    cooldown_seconds = int(_config_value("ALARM", "COOLDOWN_SECONDS", 300))
    _set_key(cooldown_key, "1", cooldown_seconds)
    return _alarm_result(event, True, methods, _message(event), notify_status, sound_status, voice_status)


def _call_sound_light(event):
    if not _config_bool("ALARM", "ENABLE_PLC_ALARM", True):
        return "not_configured:plc_disabled"

    alarm_enable_address = _config_value("ALARM", "PLC_ALARM_ENABLE_ADDRESS", DEFAULT_ALARM_ENABLE_ADDRESS)
    buzzer_address = _config_value("ALARM", "PLC_BUZZER_ADDRESS", DEFAULT_BUZZER_ADDRESS)
    buzzer_seconds = float(_config_value("ALARM", "BUZZER_SECONDS", DEFAULT_BUZZER_SECONDS))

    try:
        _write_plc_register(alarm_enable_address, 1)
        _write_plc_register(buzzer_address, 1)
        time.sleep(max(0.0, buzzer_seconds))
        _write_plc_register(buzzer_address, 0)
        return "success:plc_buzzer"
    except Exception as exc:
        return "failed:plc_buzzer:%s" % exc


def _call_voice_alarm(event):
    if not _config_bool("ALARM", "ENABLE_VOICE_ALARM", True):
        return "not_configured:voice_disabled"

    text = _message(event)
    socket_status = _send_tts_socket(text)
    local_status = _speak_text(text)
    if socket_status.startswith("success") or local_status.startswith("success"):
        return "success:%s,%s" % (socket_status, local_status)
    return "failed:%s,%s" % (socket_status, local_status)


def _call_enterprise_notify(event):
    url = _config_value("ALARM", "WEBHOOK_URL", os.getenv("ALARM_WEBHOOK_URL"))
    if not url:
        return "not_configured"

    method = _config_value("ALARM", "NOTIFY_METHOD", "wechat")
    payload = _notify_payload(method, event)
    try:
        resp = requests.post(url, json=payload, timeout=8)
        if resp.status_code >= 400:
            return "failed:%s:%s" % (resp.status_code, resp.text[:200])
        try:
            body = resp.json()
        except ValueError:
            body = {}
        if body and body.get("errcode") not in (None, 0):
            return "failed:wechat:%s:%s" % (body.get("errcode"), body.get("errmsg"))
        return "success:%s" % method
    except Exception as exc:
        return "failed:%s" % exc


def _notify_payload(method, event):
    content = _message(event)
    if method in ("wechat", "wecom", "enterprise_wechat"):
        return {
            "msgtype": "text",
            "text": {
                "content": content,
            },
        }
    if method == "dingtalk":
        return {
            "msgtype": "text",
            "text": {
                "content": content,
            },
        }
    return {
        "text": content,
        "event": event,
    }


def _write_plc_register(address, value):
    register_address = _parse_plc_address(address)
    write_value = int(value)
    host = _config_value("ALARM", "PLC_IP", DEFAULT_PLC_IP)
    port = int(_config_value("ALARM", "PLC_PORT", DEFAULT_PLC_PORT))
    timeout = float(_config_value("ALARM", "PLC_TIMEOUT", DEFAULT_PLC_TIMEOUT))
    device_id = int(_config_value("ALARM", "PLC_DEVICE_ID", DEFAULT_PLC_DEVICE_ID))

    client = ModbusTcpClient(host=host, port=port, timeout=timeout)
    try:
        if not client.connect():
            raise RuntimeError("cannot connect PLC %s:%s" % (host, port))
        response = client.write_register(address=register_address, value=write_value, device_id=device_id)
        if isinstance(response, ModbusException):
            raise RuntimeError("Modbus write failed: %s" % response)
        if response.isError():
            error_code = getattr(response, "exception_code", response)
            raise RuntimeError("PLC returned error: %s" % error_code)
    finally:
        client.close()


def _parse_plc_address(address):
    text = str(address or "").strip()
    if not text:
        raise ValueError("PLC address is required")
    if text[0].lower() == "d":
        text = text[1:]
    if not text.isdigit():
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            raise ValueError("invalid PLC address: %s" % address)
        text = digits
    return int(text)


def _send_tts_socket(text):
    host = _config_value("ALARM", "TTS_HOST", DEFAULT_TTS_HOST)
    port = int(_config_value("ALARM", "TTS_PORT", DEFAULT_TTS_PORT))
    timeout = float(_config_value("ALARM", "TTS_TIMEOUT", DEFAULT_TTS_TIMEOUT))
    payload = text if text.startswith("#") else "#%s" % text

    client_socket = None
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(timeout)
        client_socket.connect((host, port))
        client_socket.sendall(payload.encode("gb2312", errors="ignore"))
        try:
            client_socket.recv(1024)
        except socket.timeout:
            pass
        return "success:tts_socket"
    except Exception as exc:
        return "failed:tts_socket:%s" % exc
    finally:
        if client_socket:
            try:
                client_socket.close()
            except Exception:
                pass


def _speak_text(text):
    if not _config_bool("ALARM", "ENABLE_LOCAL_TTS", True):
        return "not_configured:local_tts_disabled"
    try:
        engine = pyttsx3.init()
        engine.setProperty("rate", int(_config_value("ALARM", "TTS_RATE", DEFAULT_TTS_RATE)))
        engine.setProperty("volume", float(_config_value("ALARM", "TTS_VOLUME", DEFAULT_TTS_VOLUME)))
        voices = engine.getProperty("voices")
        if voices:
            engine.setProperty("voice", voices[0].id)
        engine.say(text)
        engine.runAndWait()
        return "success:local_tts"
    except Exception as exc:
        return "failed:local_tts:%s" % exc


def _alarm_result(event, triggered, methods, message, notify_status, sound_status, voice_status):
    return {
        "monitor_id": event.get("monitor_id"),
        "event_id": event.get("event_id"),
        "alarm_triggered": triggered,
        "alarm_methods": methods,
        "notify_message": message,
        "alarm_time": _now_text(),
        "notify_status": notify_status,
        "sound_light_status": sound_status,
        "voice_status": voice_status,
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


def _config_bool(section, key, default):
    value = _config_value(section, key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in ("0", "false", "no", "off")


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "alarm_result_topic": topic, "status": status}}
