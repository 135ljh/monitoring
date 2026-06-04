#!/usr/bin/env python
# -*- coding: utf-8 -*-
import base64
import datetime as dt
import hashlib
import json
import os
import socket
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import pyttsx3
import requests
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio
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

DEFAULT_COOLDOWN_SECONDS = 60
DEFAULT_IMAGE_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_VIDEO_MAX_BYTES = 20 * 1024 * 1024

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
        methods.append(_config_value("ALARM", "NOTIFY_METHOD", "wechat"))

    if not methods:
        methods = ["record_only"]

    if _should_set_cooldown(methods, notify_status, sound_status, voice_status):
        cooldown_seconds = int(_config_value("ALARM", "COOLDOWN_SECONDS", DEFAULT_COOLDOWN_SECONDS))
        _set_key(cooldown_key, "1", cooldown_seconds)
    return _alarm_result(event, True, methods, _message(event), notify_status, sound_status, voice_status)


def _should_set_cooldown(methods, notify_status, sound_status, voice_status):
    if methods == ["record_only"]:
        return False
    return any(str(status).startswith("success") for status in (notify_status, sound_status, voice_status))


def _call_sound_light(event):
    if not _config_bool("ALARM", "ENABLE_PLC_ALARM", False):
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
    if not _config_bool("ALARM", "ENABLE_VOICE_ALARM", False):
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
    if method not in ("wechat", "wecom", "enterprise_wechat"):
        return _send_webhook_payload(url, _notify_payload(method, event), method)

    statuses = []
    statuses.append("text=%s" % _send_webhook_payload(url, _notify_payload(method, event), method))

    if _config_bool("ALARM", "SEND_EVIDENCE_IMAGE", True):
        statuses.append("image=%s" % _send_wechat_image(url, event.get("evidence_frame_url")))

    if _config_bool("ALARM", "SEND_EVIDENCE_VIDEO", True):
        statuses.append("video=%s" % _send_wechat_file(url, event.get("evidence_video_url"), event))

    if any(item.endswith("success:wechat") or "=success:wechat" in item for item in statuses):
        return "success:wechat:%s" % ";".join(statuses)
    return "failed:wechat:%s" % ";".join(statuses)


def _send_webhook_payload(url, payload, method):
    try:
        resp = requests.post(
            url,
            data=json.dumps(payload, ensure_ascii=True).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=8,
        )
        if resp.status_code >= 400:
            return "failed:%s:%s" % (resp.status_code, resp.text[:200])
        body = _json_or_empty(resp)
        if body and body.get("errcode") not in (None, 0):
            return "failed:%s:%s" % (body.get("errcode"), body.get("errmsg"))
        return "success:%s" % method
    except Exception as exc:
        return "failed:%s" % exc


def _notify_payload(method, event):
    content = _message(event)
    if method in ("wechat", "wecom", "enterprise_wechat", "dingtalk"):
        return {"msgtype": "text", "text": {"content": content}}
    return {"text": content, "event": event}


def _send_wechat_image(url, image_url):
    if not image_url:
        return "skipped:no_image_url"
    try:
        image_bytes = _download_bytes(image_url)
        max_bytes = int(_config_value("ALARM", "IMAGE_MAX_BYTES", DEFAULT_IMAGE_MAX_BYTES))
        if len(image_bytes) > max_bytes:
            return "skipped:image_too_large:%s" % len(image_bytes)
        payload = {
            "msgtype": "image",
            "image": {
                "base64": base64.b64encode(image_bytes).decode("ascii"),
                "md5": hashlib.md5(image_bytes).hexdigest(),
            },
        }
        return _send_webhook_payload(url, payload, "wechat")
    except Exception as exc:
        return "failed:%s" % exc


def _send_wechat_file(url, file_url, event):
    if not file_url:
        return "skipped:no_video_url"

    key = _wechat_webhook_key(url)
    if not key:
        return "skipped:no_webhook_key"

    suffix = Path(urlparse(file_url).path).suffix or ".mp4"
    filename = "%s_%s%s" % (event.get("event_id", "event"), event.get("clip_id", "clip"), suffix)
    target = Path(tempfile.gettempdir()) / filename
    try:
        _download_file(file_url, target)
        max_bytes = int(_config_value("ALARM", "VIDEO_MAX_BYTES", DEFAULT_VIDEO_MAX_BYTES))
        size = target.stat().st_size
        if size > max_bytes:
            return "skipped:video_too_large:%s" % size
        media_id = _upload_wechat_media(key, target)
        payload = {"msgtype": "file", "file": {"media_id": media_id}}
        return _send_webhook_payload(url, payload, "wechat")
    except Exception as exc:
        return "failed:%s" % exc
    finally:
        _safe_unlink(target)


def _upload_wechat_media(key, file_path):
    upload_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media"
    with open(file_path, "rb") as fh:
        resp = requests.post(
            upload_url,
            params={"key": key, "type": "file"},
            files={"media": (file_path.name, fh, "application/octet-stream")},
            timeout=30,
        )
    if resp.status_code >= 400:
        raise RuntimeError("upload_media failed: %s %s" % (resp.status_code, resp.text[:200]))
    body = _json_or_empty(resp)
    if body.get("errcode") not in (None, 0):
        raise RuntimeError("upload_media failed: %s %s" % (body.get("errcode"), body.get("errmsg")))
    media_id = body.get("media_id")
    if not media_id:
        raise RuntimeError("upload_media missing media_id")
    return media_id


def _wechat_webhook_key(url):
    return (parse_qs(urlparse(url).query).get("key") or [""])[0]


def _download_bytes(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("unsupported evidence url: %s" % url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.content
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 403:
            raise
        bucket, object_name = _minio_object_from_url(url)
        response = _minio_client().get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()


def _download_file(url, target):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError("unsupported evidence url: %s" % url)
    try:
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            with open(target, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
    except requests.HTTPError as exc:
        if exc.response is None or exc.response.status_code != 403:
            raise
        bucket, object_name = _minio_object_from_url(url)
        _minio_client().fget_object(bucket, object_name, str(target))


def _minio_object_from_url(url):
    parts = [unquote(item) for item in urlparse(url).path.strip("/").split("/") if item]
    if len(parts) < 2:
        raise ValueError("invalid MinIO url: %s" % url)
    return parts[0], "/".join(parts[1:])


def _minio_client():
    endpoint = _config_value("MINIO", "ENDPOINT", os.getenv("MINIO_ENDPOINT", "10.21.221.12:9000"))
    access_key = _config_value("MINIO", "ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    secret_key = _config_value("MINIO", "SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "Admin@hd2019"))
    secure = str(_config_value("MINIO", "SECURE", os.getenv("MINIO_SECURE", "false"))).lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


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
    lines = [
        _u("车间检测到异常行为：%s。请及时处理。") % _event_description(event),
        _u("事件时间：%s") % (event.get("event_time") or _now_text()),
        _u("异常等级：%s") % event.get("abnormal_level", "unknown"),
        _u("异常类型：%s") % ", ".join(event.get("abnormal_types", []) or ["unknown"]),
    ]
    if event.get("camera_id"):
        lines.append(_u("摄像头：%s") % event.get("camera_id"))
    if event.get("clip_id"):
        lines.append(_u("视频片段：%s") % event.get("clip_id"))
    if event.get("evidence_frame_url"):
        lines.append(_u("证据图片：%s") % event.get("evidence_frame_url"))
    if event.get("evidence_video_url"):
        lines.append(_u("证据视频：%s") % event.get("evidence_video_url"))
    return "\n".join(lines)


def _event_description(event):
    description = str(event.get("event_description") or "").strip()
    if description and not _looks_garbled(description):
        return description

    names = []
    for item in event.get("abnormal_types", []) or []:
        names.append(_type_name(item))
    return _u("，").join(names) if names else _u("未知异常")


def _type_name(item):
    return {
        "person_static": _u("人员长时间静止"),
        "person_fall": _u("人员疑似跌倒"),
        "person_intrusion": _u("人员进入危险区域"),
        "device_vibration": _u("设备异常震动"),
        "device_stop": _u("设备异常停机"),
        "unknown_abnormal": _u("未知异常"),
    }.get(item, item)


def _looks_garbled(text):
    if not text:
        return True
    question_count = text.count("?") + text.count(_u("？"))
    return question_count >= 3 or question_count >= max(1, len(text) // 3)


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


def _json_or_empty(resp):
    try:
        return resp.json()
    except ValueError:
        return {}


def _safe_unlink(path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def _u(text):
    return text


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _now_text():
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "alarm_result_topic": topic, "status": status}}
