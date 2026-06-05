#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import time
import threading
import uuid

import pymysql
from kafka import KafkaConsumer, KafkaProducer
from pymodbus.client import ModbusTcpClient
from pymodbus.exceptions import ModbusException


RECOGNITION_RESULT_TOPIC = "workshop.recognition_result"
ABNORMAL_EVENT_TOPIC = "workshop.abnormal_event"
DEFAULT_GROUP = "workshop-abnormal-judgement"

DEFAULT_STATIC_SECONDS = 10.0
DEFAULT_FALL_SECONDS = 3.0
DEFAULT_ABNORMAL_POSTURE_SECONDS = 20.0
DEFAULT_PERSON_ABSENT_SECONDS = 60.0
DEFAULT_CROWD_SECONDS = 10.0
DEFAULT_RUNNING_SECONDS = 2.0
DEFAULT_HELP_GESTURE_SECONDS = 2.0
DEFAULT_FALL_NO_MOVEMENT_SECONDS = 10.0
DEFAULT_CROWD_PERSON_THRESHOLD = 5
DEFAULT_VIBRATION_DANGER_THRESHOLD = 0.006
DEFAULT_SENSOR_POLL_INTERVAL = 2.0
DEFAULT_SENSOR_PLC_IP = "10.21.2.233"
DEFAULT_SENSOR_PLC_PORT = 502
DEFAULT_SENSOR_PLC_TIMEOUT = 3.0
DEFAULT_SENSOR_PLC_DEVICE_ID = 1

DEFAULT_SENSOR_POINTS = [
    {"code": "voltage", "name": "\u7535\u538b", "address": "D2000", "scale": 0.1, "unit": "V", "min": 180.0, "max": 260.0},
    {"code": "current", "name": "\u7535\u6d41", "address": "D2001", "scale": 0.01, "unit": "A", "min": 0.0, "max": 20.0},
    {"code": "active_power", "name": "\u77ac\u65f6\u6709\u529f\u529f\u7387", "address": "D2002", "scale": 1.0, "unit": "W", "min": 0.0, "max": 5000.0},
    {"code": "power_factor", "name": "\u529f\u7387\u56e0\u6570", "address": "D2003", "scale": 0.001, "unit": "COS", "min": 0.8, "max": 1.0},
    {"code": "frequency", "name": "\u9891\u7387", "address": "D2004", "scale": 0.01, "unit": "Hz", "min": 49.0, "max": 51.0},
    {"code": "total_energy", "name": "\u603b\u6709\u529f\u7535\u80fd", "address": "D2005", "scale": 0.01, "unit": "kWh"},
    {"code": "total_water", "name": "\u603b\u7528\u6c34\u91cf", "address": "D2010", "scale": 0.01, "unit": "m3"},
    {"code": "humidity", "name": "\u6e7f\u5ea6", "address": "D2020", "scale": 0.1, "unit": "%RH", "min": 20.0, "max": 85.0},
    {"code": "temperature", "name": "\u6e29\u5ea6", "address": "D2021", "scale": 0.1, "unit": "\u2103", "min": 0.0, "max": 45.0},
    {"code": "noise", "name": "\u566a\u97f3", "address": "D2030", "scale": 0.1, "unit": "dB", "min": 0.0, "max": 85.0},
    {"code": "smoke", "name": "\u70df\u96fe", "address": "D2040", "scale": 1.0, "unit": "ppm", "min": 0.0, "max": 100.0},
    {"code": "rope_displacement", "name": "\u62c9\u7ef3\u4f4d\u79fb", "address": "D2060", "scale": 0.1, "unit": "mm", "min": 0.0, "max": 1000.0},
    {"code": "illuminance", "name": "\u5149\u7167\u5ea6", "address": "D2140", "scale": 1.0, "unit": "Lux", "min": 50.0, "max": 2000.0},
    {"code": "safety_grating", "name": "\u5b89\u5168\u5149\u6805", "address": "D2145", "scale": 1.0, "unit": "", "normal": 1},
]

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
        if _config_bool("SENSOR", "ENABLED", True):
            sensor_thread = threading.Thread(
                target=_sensor_poll_loop,
                args=(task,),
                daemon=True,
                name="sensor-monitor-%s" % monitor_id,
            )
            task["sensor_thread"] = sensor_thread
            sensor_thread.start()

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


def _sensor_poll_loop(task):
    producer = None
    try:
        _init_mysql()
        producer = _create_producer()
        interval = float(_config_value("SENSOR", "POLL_INTERVAL_SECONDS", DEFAULT_SENSOR_POLL_INTERVAL))
        while not task["stop_event"].is_set():
            started = time.time()
            try:
                readings = _read_sensor_values()
                if readings:
                    _save_sensor_readings(task["monitor_id"], readings)
                    events = _sensor_events(task["monitor_id"], readings)
                    for event in events:
                        _save_event(event)
                        _save_sensor_abnormal(event)
                        producer.send(task["output_topic"], event)
                        producer.flush(timeout=10)
                        print("published sensor abnormal event: %s" % json.dumps(event, ensure_ascii=False))
            except Exception as exc:
                print("sensor monitor failed: %s" % exc)
            elapsed = time.time() - started
            time.sleep(max(0.2, interval - elapsed))
    except Exception as exc:
        print("sensor monitor task failed: %s" % exc)
    finally:
        if producer:
            producer.close()


def _read_sensor_values():
    points = _sensor_points()
    if not points:
        return []

    host = _config_value("SENSOR", "PLC_IP", DEFAULT_SENSOR_PLC_IP)
    port = int(_config_value("SENSOR", "PLC_PORT", DEFAULT_SENSOR_PLC_PORT))
    timeout = float(_config_value("SENSOR", "PLC_TIMEOUT", DEFAULT_SENSOR_PLC_TIMEOUT))
    device_id = int(_config_value("SENSOR", "PLC_DEVICE_ID", DEFAULT_SENSOR_PLC_DEVICE_ID))

    addresses = sorted(set(address for point in points for address in _point_addresses(point)))
    register_values = _read_sensor_registers(host, port, timeout, device_id, addresses)

    now = _now_text()
    readings = []
    for point in points:
        address = _parse_plc_address(point["address"])
        words = [register_values.get(address + offset) for offset in range(int(point.get("word_count", 1)))]
        if any(word is None for word in words):
            continue
        raw_value = _decode_sensor_words(words, point)
        value = round(raw_value * float(point.get("scale", 1.0)), 4)
        status, reason = _sensor_status(point, value)
        readings.append({
            "panel_no": str(_config_value("SENSOR", "PANEL_NO", "4")),
            "sensor_code": point.get("code"),
            "sensor_name": point.get("name"),
            "address": point.get("address"),
            "raw_value": raw_value,
            "value": value,
            "unit": point.get("unit", ""),
            "threshold_min": point.get("min"),
            "threshold_max": point.get("max"),
            "normal_value": point.get("normal"),
            "status": status,
            "abnormal_reason": reason,
            "collected_at": now,
        })
    return readings


def _read_sensor_registers(host, port, timeout, device_id, addresses):
    if not addresses:
        return {}
    if str(_config_value("SENSOR", "READ_MODE", "single")).lower() == "block":
        return _read_sensor_registers_block(host, port, timeout, device_id, addresses)
    return _read_sensor_registers_single(host, port, timeout, device_id, addresses)


def _read_sensor_registers_single(host, port, timeout, device_id, addresses):
    values = {}
    client = ModbusTcpClient(host=host, port=port, timeout=timeout)
    try:
        if not client.connect():
            raise RuntimeError("cannot connect sensor PLC %s:%s" % (host, port))
        for address in addresses:
            response = client.read_holding_registers(address=address, count=1, device_id=device_id)
            if isinstance(response, ModbusException):
                raise RuntimeError("Modbus read failed at D%s: %s" % (address, response))
            if response.isError():
                error_code = getattr(response, "exception_code", response)
                raise RuntimeError("PLC returned error at D%s: %s" % (address, error_code))
            values[address] = response.registers[0]
        return values
    finally:
        client.close()


def _read_sensor_registers_block(host, port, timeout, device_id, addresses):
    values = {}
    chunk_size = int(_config_value("SENSOR", "READ_CHUNK_SIZE", 20))
    start = addresses[0]
    end = addresses[0]
    groups = []
    for address in addresses[1:]:
        if address == end + 1 and (address - start + 1) <= chunk_size:
            end = address
        else:
            groups.append((start, end))
            start = address
            end = address
    groups.append((start, end))
    for start, end in groups:
        values.update(_read_holding_register_block(host, port, timeout, device_id, start, end - start + 1))
    return values


def _read_holding_register_block(host, port, timeout, device_id, start, count):
    client = ModbusTcpClient(host=host, port=port, timeout=timeout)
    values = {}
    try:
        if not client.connect():
            raise RuntimeError("cannot connect sensor PLC %s:%s" % (host, port))
        remaining = count
        offset = 0
        chunk_size = int(_config_value("SENSOR", "READ_CHUNK_SIZE", 100))
        while remaining > 0:
            current = min(chunk_size, remaining)
            response = client.read_holding_registers(address=start + offset, count=current, device_id=device_id)
            if isinstance(response, ModbusException):
                raise RuntimeError("Modbus read failed: %s" % response)
            if response.isError():
                error_code = getattr(response, "exception_code", response)
                raise RuntimeError("PLC returned error: %s" % error_code)
            for idx, value in enumerate(response.registers):
                values[start + offset + idx] = value
            offset += current
            remaining -= current
        return values
    finally:
        client.close()


def _sensor_points():
    configured = _config_value("SENSOR", "POINTS", None)
    if isinstance(configured, list) and configured:
        return configured
    return DEFAULT_SENSOR_POINTS


def _sensor_status(point, value):
    normal = point.get("normal")
    if normal is not None and value != float(normal):
        return "abnormal", "%s=%s, expected %s" % (point.get("name"), value, normal)
    min_value = point.get("min")
    if min_value is not None and value < float(min_value):
        return "abnormal", "%s %.4f%s below %.4f%s" % (
            point.get("name"), value, point.get("unit", ""), float(min_value), point.get("unit", ""))
    max_value = point.get("max")
    if max_value is not None and value > float(max_value):
        return "abnormal", "%s %.4f%s above %.4f%s" % (
            point.get("name"), value, point.get("unit", ""), float(max_value), point.get("unit", ""))
    return "normal", ""


def _sensor_events(monitor_id, readings):
    events = []
    for reading in readings:
        if reading.get("status") != "abnormal":
            continue
        if _sensor_in_cooldown(monitor_id, reading):
            continue
        level = _sensor_level(reading)
        event = {
            "monitor_id": monitor_id,
            "event_id": "evt_%s" % uuid.uuid4().hex[:12],
            "is_abnormal": True,
            "abnormal_types": ["sensor_abnormal"],
            "abnormal_level": level,
            "event_description": _sensor_event_description(reading),
            "evidence_video_url": None,
            "evidence_frame_url": None,
            "event_time": reading.get("collected_at") or _now_text(),
            "camera_id": None,
            "clip_id": None,
            "sensor_data": reading,
        }
        events.append(event)
        _set_key(_sensor_cooldown_key(monitor_id, reading), "1", int(_config_value("SENSOR", "COOLDOWN_SECONDS", 60)))
    return events


def _sensor_event_description(reading):
    unit = reading.get("unit") or ""
    parts = [
        "\u4f20\u611f\u5668%s\u5f02\u5e38" % reading.get("sensor_name"),
        "\u5730\u5740%s" % reading.get("address"),
        "\u5f53\u524d\u503c%.4f%s" % (float(reading.get("value", 0.0)), unit),
    ]
    if reading.get("threshold_min") is not None:
        parts.append("\u4e0b\u9650%.4f%s" % (float(reading.get("threshold_min")), unit))
    if reading.get("threshold_max") is not None:
        parts.append("\u4e0a\u9650%.4f%s" % (float(reading.get("threshold_max")), unit))
    if reading.get("normal_value") is not None:
        parts.append("\u6b63\u5e38\u503c%s" % reading.get("normal_value"))
    return "\uff0c".join(parts)


def _sensor_level(reading):
    code = reading.get("sensor_code")
    if code in ("smoke", "safety_grating"):
        return "high"
    if code in ("voltage", "current", "temperature"):
        return "medium"
    return "low"


def _sensor_in_cooldown(monitor_id, reading):
    return bool(_get_key(_sensor_cooldown_key(monitor_id, reading)))


def _sensor_cooldown_key(monitor_id, reading):
    return "monitor:%s:sensor:%s:cooldown" % (monitor_id, reading.get("sensor_code", "unknown"))


def _parse_plc_address(address):
    text = str(address or "").strip()
    if text[:1].lower() == "d":
        text = text[1:]
    if not text.isdigit():
        raise ValueError("invalid PLC address: %s" % address)
    return int(text)


def _point_addresses(point):
    start = _parse_plc_address(point["address"])
    count = int(point.get("word_count", 1))
    return [start + offset for offset in range(max(1, count))]


def _decode_sensor_words(words, point):
    values = [int(word) for word in words]
    if int(point.get("word_count", 1)) <= 1:
        value = values[0]
        return _to_signed_16(value) if _sensor_bool(point, "signed", False) else value

    order = str(point.get("word_order", _config_value("SENSOR", "DINT_WORD_ORDER", "high_low"))).lower()
    if order in ("low_high", "little", "little_endian"):
        combined = (values[1] << 16) | values[0]
    else:
        combined = (values[0] << 16) | values[1]
    return _to_signed_32(combined) if _sensor_bool(point, "signed", True) else combined


def _to_signed_16(value):
    value = int(value)
    return value - 65536 if value >= 32768 else value


def _to_signed_32(value):
    value = int(value)
    return value - 4294967296 if value >= 2147483648 else value


def _sensor_bool(point, key, default):
    value = point.get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _judge_message(msg):
    monitor_id = msg["monitor_id"]
    abnormal_types = []
    descriptions = []
    scene = msg.get("scene_result", {}) or {}

    for person in msg.get("person_results", []):
        person_id = person.get("person_id", "unknown")
        _judge_person_static(monitor_id, msg, person, person_id, abnormal_types, descriptions)
        _judge_person_fall(monitor_id, msg, person, person_id, abnormal_types, descriptions)
        _judge_abnormal_posture(monitor_id, msg, person, person_id, abnormal_types, descriptions)
        _judge_person_running(monitor_id, msg, person, person_id, abnormal_types, descriptions)
        _judge_help_gesture(monitor_id, msg, person, person_id, abnormal_types, descriptions)
        _judge_fall_no_movement(monitor_id, msg, person, person_id, abnormal_types, descriptions)

    _judge_person_absent(monitor_id, msg, scene, abnormal_types, descriptions)
    _judge_crowd_gathering(monitor_id, msg, scene, abnormal_types, descriptions)
    _judge_device_vibration(monitor_id, msg, abnormal_types, descriptions)

    abnormal_types = sorted(set(abnormal_types))
    if not abnormal_types:
        return None

    return {
        "monitor_id": monitor_id,
        "event_id": "evt_%s" % uuid.uuid4().hex[:12],
        "is_abnormal": True,
        "abnormal_types": abnormal_types,
        "abnormal_level": _event_level(abnormal_types),
        "event_description": "\uff0c".join(descriptions) if descriptions else "\u68c0\u6d4b\u5230\u5f02\u5e38\u884c\u4e3a",
        "evidence_video_url": msg.get("annotated_video_url"),
        "evidence_frame_url": msg.get("annotated_frame_url"),
        "event_time": _now_text(),
        "camera_id": msg.get("camera_id"),
        "clip_id": msg.get("clip_id"),
    }


def _judge_person_static(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:static" % person_id
    if person.get("action_type") != "static":
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "STATIC_SECONDS", DEFAULT_STATIC_SECONDS)):
        abnormal_types.append("person_static")
        descriptions.append("\u4eba\u5458%s\u9759\u6b62\u8d85\u8fc7%.1f\u79d2" % (person_id, duration))


def _judge_person_fall(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:fall" % person_id
    if not person.get("fall_suspected"):
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "FALL_SECONDS", DEFAULT_FALL_SECONDS)):
        abnormal_types.append("person_fall")
        descriptions.append("\u4eba\u5458%s\u7591\u4f3c\u8dcc\u5012\u6301\u7eed%.1f\u79d2" % (person_id, duration))


def _judge_abnormal_posture(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:abnormal_posture" % person_id
    posture_type = person.get("posture_type")
    if posture_type not in ("bend", "squat"):
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "ABNORMAL_POSTURE_SECONDS", DEFAULT_ABNORMAL_POSTURE_SECONDS)):
        abnormal_types.append("abnormal_posture")
        descriptions.append("\u4eba\u5458%s\u957f\u65f6\u95f4%s\u59ff\u6001\u8d85\u8fc7%.1f\u79d2" % (
            person_id, _posture_name(posture_type), duration))


def _judge_person_running(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:running" % person_id
    if not person.get("running_suspected"):
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "RUNNING_SECONDS", DEFAULT_RUNNING_SECONDS)):
        abnormal_types.append("person_running")
        descriptions.append("\u4eba\u5458%s\u5feb\u901f\u79fb\u52a8\u6301\u7eed%.1f\u79d2" % (person_id, duration))


def _judge_help_gesture(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:help_gesture" % person_id
    if not person.get("help_gesture_suspected"):
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "HELP_GESTURE_SECONDS", DEFAULT_HELP_GESTURE_SECONDS)):
        abnormal_types.append("help_gesture")
        descriptions.append("\u4eba\u5458%s\u7591\u4f3c\u6325\u624b\u6c42\u52a9" % person_id)


def _judge_fall_no_movement(monitor_id, msg, person, person_id, abnormal_types, descriptions):
    condition = "person:%s:fall_no_movement" % person_id
    motion_threshold = float(_config_value("JUDGE", "FALL_NO_MOVEMENT_MOTION_THRESHOLD", 0.012))
    if not person.get("fall_suspected") or float(person.get("movement_score", 0.0) or 0.0) >= motion_threshold:
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "FALL_NO_MOVEMENT_SECONDS", DEFAULT_FALL_NO_MOVEMENT_SECONDS)):
        abnormal_types.append("fall_no_movement")
        descriptions.append("\u4eba\u5458%s\u5012\u5730\u540e%.1f\u79d2\u5185\u51e0\u4e4e\u65e0\u52a8\u4f5c" % (person_id, duration))


def _judge_person_absent(monitor_id, msg, scene, abnormal_types, descriptions):
    condition = "scene:person_absent"
    person_count = int(scene.get("person_count", 0) or 0)
    if person_count != 0:
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "PERSON_ABSENT_SECONDS", DEFAULT_PERSON_ABSENT_SECONDS)):
        abnormal_types.append("person_absent")
        descriptions.append("\u6307\u5b9a\u5de5\u4f4d\u533a\u57df\u8fde\u7eed%.1f\u79d2\u672a\u68c0\u6d4b\u5230\u4eba\u5458" % duration)


def _judge_crowd_gathering(monitor_id, msg, scene, abnormal_types, descriptions):
    condition = "scene:crowd_gathering"
    person_count = int(scene.get("crowd_count", scene.get("person_count", 0)) or 0)
    threshold = int(_config_value("JUDGE", "CROWD_PERSON_THRESHOLD", DEFAULT_CROWD_PERSON_THRESHOLD))
    if person_count <= threshold:
        _delete_condition(monitor_id, condition)
        return
    duration = _accumulate_condition_duration(monitor_id, condition, msg)
    if duration >= float(_config_value("JUDGE", "CROWD_SECONDS", DEFAULT_CROWD_SECONDS)):
        abnormal_types.append("crowd_gathering")
        descriptions.append("\u540c\u4e00\u533a\u57df\u4eba\u6570%d\u4eba\u8d85\u8fc7\u9608\u503c%d\u4eba\u5e76\u6301\u7eed%.1f\u79d2" % (
            person_count, threshold, duration))


def _judge_device_vibration(monitor_id, msg, abnormal_types, descriptions):
    for device in msg.get("device_results", []):
        device_id = device.get("device_id", "unknown")
        vibration_score = float(device.get("vibration_score", 0.0) or 0.0)
        _push_history("monitor:%s:device:%s:vibration_history" % (monitor_id, device_id), vibration_score)
        danger_threshold = float(_config_value("JUDGE", "VIBRATION_DANGER_THRESHOLD", DEFAULT_VIBRATION_DANGER_THRESHOLD))
        if device.get("vibration_level") == "danger" or vibration_score >= danger_threshold:
            abnormal_types.append("device_vibration")
            descriptions.append("\u8bbe\u5907%s\u5b58\u5728\u5f02\u5e38\u9707\u52a8\uff0c\u9707\u52a8\u5206\u6570%.4f" % (
                device_id, vibration_score))


def _accumulate_static_duration(monitor_id, person_id, msg):
    return _accumulate_condition_duration(monitor_id, "person:%s:static" % person_id, msg)


def _accumulate_condition_duration(monitor_id, condition_key, msg):
    start_key = "monitor:%s:%s:start_time" % (monitor_id, condition_key)
    duration_key = "monitor:%s:%s:duration" % (monitor_id, condition_key)
    seen_key = "monitor:%s:%s:last_seen_time" % (monitor_id, condition_key)
    now = _parse_time(msg.get("end_time")) or dt.datetime.now()

    start_raw = _get_key(start_key)
    if start_raw:
        start = _parse_time(start_raw) or now
    else:
        start = _parse_time(msg.get("start_time")) or now
        _set_key(start_key, _format_dt(start), 86400)

    duration = max(0.0, (now - start).total_seconds())
    _set_key(duration_key, duration, 86400)
    _set_key(seen_key, _format_dt(now), 86400)
    return duration


def _delete_condition(monitor_id, condition_key):
    _delete_key("monitor:%s:%s:start_time" % (monitor_id, condition_key))
    _delete_key("monitor:%s:%s:duration" % (monitor_id, condition_key))
    _delete_key("monitor:%s:%s:last_seen_time" % (monitor_id, condition_key))


def _posture_name(value):
    return {"bend": "\u5f2f\u8170", "squat": "\u8e72\u4e0b", "horizontal": "\u6a2a\u5411"}.get(value, value)


def _event_level(types):
    high_types = {"device_vibration", "person_fall", "fall_no_movement"}
    medium_types = {
        "person_static",
        "abnormal_posture",
        "person_absent",
        "crowd_gathering",
        "person_running",
        "help_gesture",
    }
    if high_types.intersection(types):
        return "high"
    if medium_types.intersection(types):
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sensor_abnormal_monitor (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    monitor_id VARCHAR(128),
                    event_id VARCHAR(128) UNIQUE,
                    panel_no VARCHAR(32),
                    sensor_code VARCHAR(128),
                    sensor_name VARCHAR(128),
                    address VARCHAR(32),
                    value DOUBLE,
                    unit VARCHAR(32),
                    threshold_min DOUBLE NULL,
                    threshold_max DOUBLE NULL,
                    normal_value VARCHAR(64),
                    abnormal_reason TEXT,
                    abnormal_level VARCHAR(32),
                    collected_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sensor_abnormal_collected_at (sensor_code, collected_at)
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


def _save_sensor_readings(monitor_id, readings):
    conn = _mysql_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            rows = []
            for item in readings:
                rows.append((
                    monitor_id,
                    item.get("panel_no"),
                    item.get("sensor_code"),
                    item.get("sensor_name"),
                    item.get("address"),
                    item.get("raw_value"),
                    item.get("value"),
                    item.get("unit"),
                    item.get("status"),
                    item.get("collected_at"),
                ))
            if rows:
                cur.executemany("""
                    INSERT INTO sensor_data_record (
                        monitor_id, panel_no, sensor_code, sensor_name, address,
                        raw_value, value, unit, status, collected_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, rows)
        conn.commit()
    except Exception as exc:
        print("save sensor readings failed: %s" % exc)
    finally:
        conn.close()


def _save_sensor_abnormal(event):
    sensor = event.get("sensor_data") or {}
    if not sensor:
        return
    conn = _mysql_conn()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sensor_abnormal_monitor (
                    monitor_id, event_id, panel_no, sensor_code, sensor_name, address,
                    value, unit, threshold_min, threshold_max, normal_value,
                    abnormal_reason, abnormal_level, collected_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE abnormal_reason=VALUES(abnormal_reason)
            """, (
                event.get("monitor_id"),
                event.get("event_id"),
                sensor.get("panel_no"),
                sensor.get("sensor_code"),
                sensor.get("sensor_name"),
                sensor.get("address"),
                sensor.get("value"),
                sensor.get("unit"),
                sensor.get("threshold_min"),
                sensor.get("threshold_max"),
                sensor.get("normal_value"),
                sensor.get("abnormal_reason"),
                event.get("abnormal_level"),
                sensor.get("collected_at"),
            ))
        conn.commit()
    except Exception as exc:
        print("save sensor abnormal failed: %s" % exc)
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


def _config_bool(section, key, default):
    value = _config_value(section, key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


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
