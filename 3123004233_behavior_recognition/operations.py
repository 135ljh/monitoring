#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import tempfile
import threading
import uuid
from pathlib import Path
from urllib.parse import urlparse

import cv2
import numpy as np
import requests
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio


PROCESSED_VIDEO_TOPIC = "workshop.processed_video"
RECOGNITION_RESULT_TOPIC = "workshop.recognition_result"
DEFAULT_GROUP = "workshop-behavior-recognition"

_TASKS = {}
_LOCK = threading.Lock()


def start_recognize(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    input_topic = (data or {}).get("processed_video_topic") or _config_value(
        "KAFKA", "PROCESSED_VIDEO_TOPIC", PROCESSED_VIDEO_TOPIC)
    output_topic = _config_value("KAFKA", "RECOGNITION_RESULT_TOPIC", RECOGNITION_RESULT_TOPIC)

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
        thread = threading.Thread(target=_consume_loop, args=(task,), daemon=True, name="behavior-recognition-%s" % monitor_id)
        task["thread"] = thread
        thread.start()

    _set_redis_status(monitor_id, "running")
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
            msg = record.value
            if msg.get("monitor_id") != task["monitor_id"]:
                continue
            try:
                result = _recognize_message(msg)
                producer.send(task["output_topic"], result)
                producer.flush(timeout=10)
                print("published recognition result: %s" % json.dumps(result, ensure_ascii=False))
            except Exception as exc:
                print("recognition failed: %s" % exc)
    except Exception as exc:
        print("behavior recognition task failed: %s" % exc)
        _update_task(task["monitor_id"], status="error", last_error=str(exc))
        _set_redis_status(task["monitor_id"], "error")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()


def _recognize_message(msg):
    monitor_id = msg["monitor_id"]
    camera_id = msg.get("camera_id", "CAM_001")
    clip_id = msg["clip_id"]
    temp_dir = Path(tempfile.gettempdir()) / "workshop_behavior_recognition" / monitor_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    source_path = temp_dir / ("%s_processed.mp4" % clip_id)
    annotated_path = temp_dir / ("%s_annotated.mp4" % clip_id)
    annotated_frame_path = temp_dir / ("%s_annotated.jpg" % clip_id)

    _download_url(msg["processed_video_url"], source_path)
    analysis = _analyze_video(source_path, annotated_path, annotated_frame_path)

    bucket = _config_value("MINIO", "BUCKET", "workshop")
    prefix = _config_value("MINIO", "PREFIX", "recognition").strip("/")
    object_prefix = "%s/%s/%s" % (prefix, camera_id, monitor_id)
    annotated_video_url = _upload_to_minio(annotated_path, bucket, "%s/%s.mp4" % (object_prefix, clip_id))
    annotated_frame_url = _upload_to_minio(annotated_frame_path, bucket, "%s/%s.jpg" % (object_prefix, clip_id))

    _safe_unlink(source_path)
    _safe_unlink(annotated_path)
    _safe_unlink(annotated_frame_path)

    return {
        "monitor_id": monitor_id,
        "camera_id": camera_id,
        "clip_id": clip_id,
        "recognition_id": "rec_%s" % uuid.uuid4().hex[:12],
        "start_time": msg.get("start_time"),
        "end_time": msg.get("end_time"),
        "person_results": analysis["person_results"],
        "device_results": analysis["device_results"],
        "annotated_video_url": annotated_video_url,
        "annotated_frame_url": annotated_frame_url,
    }


def _analyze_video(source_path, annotated_path, annotated_frame_path):
    cap = cv2.VideoCapture(str(source_path))
    if not cap.isOpened():
        raise RuntimeError("cannot open processed video: %s" % source_path)

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    ok, frame = cap.read()
    if not ok or frame is None:
        cap.release()
        raise RuntimeError("processed video has no frames: %s" % source_path)

    height, width = frame.shape[:2]
    writer = cv2.VideoWriter(str(annotated_path), cv2.VideoWriter_fourcc(*"mp4v"), fps, (width, height))
    if not writer.isOpened():
        cap.release()
        raise RuntimeError("cannot create annotated video: %s" % annotated_path)

    hog = cv2.HOGDescriptor()
    hog.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())

    prev_gray = None
    motion_scores = []
    vibration_scores = []
    person_boxes = []
    frame_index = 0

    device_roi = _device_roi(width, height)

    try:
        while ok and frame is not None:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            if prev_gray is not None:
                diff = cv2.absdiff(prev_gray, gray)
                motion_scores.append(float(np.mean(diff)) / 255.0)
                vibration_scores.append(_optical_flow_score(prev_gray, gray, device_roi))

            if frame_index % max(1, int(fps)) == 0:
                boxes, weights = hog.detectMultiScale(frame, winStride=(8, 8), padding=(8, 8), scale=1.05)
                for box, weight in zip(boxes, weights):
                    if float(weight) >= 0.2:
                        person_boxes.append([int(v) for v in box])

            annotated = frame.copy()
            for box in person_boxes[-3:]:
                x, y, w, h = box
                cv2.rectangle(annotated, (x, y), (x + w, y + h), (0, 0, 255), 2)
            x, y, w, h = device_roi
            cv2.rectangle(annotated, (x, y), (x + w, y + h), (0, 180, 255), 2)
            writer.write(annotated)
            if not annotated_frame_path.exists():
                cv2.imwrite(str(annotated_frame_path), annotated)

            prev_gray = gray
            frame_index += 1
            ok, frame = cap.read()
    finally:
        writer.release()
        cap.release()

    movement_score = round(float(np.mean(motion_scores)) if motion_scores else 0.0, 4)
    vibration_score = round(float(np.mean(vibration_scores)) if vibration_scores else 0.0, 4)
    action_type = "static" if movement_score < float(_config_value("RECOGNITION", "STATIC_MOTION_THRESHOLD", 0.018)) else "moving"
    vibration_level = _vibration_level(vibration_score)

    person_results = []
    for idx, box in enumerate(_dedupe_boxes(person_boxes)[:5] or [[0, 0, width, height]]):
        x, y, w, h = box
        person_results.append({
            "person_id": "P%03d" % (idx + 1),
            "bbox": [x, y, x + w, y + h],
            "action_type": action_type,
            "movement_score": movement_score,
            "confidence": 0.65 if person_boxes else 0.35,
        })

    device_results = [{
        "device_id": _config_value("RECOGNITION", "DEVICE_ID", "DEV_001"),
        "roi": {"x": device_roi[0], "y": device_roi[1], "w": device_roi[2], "h": device_roi[3]},
        "vibration_score": vibration_score,
        "vibration_level": vibration_level,
        "optical_flow_value": round(vibration_score * 100.0, 4),
    }]

    return {"person_results": person_results, "device_results": device_results}


def _optical_flow_score(prev_gray, gray, roi):
    x, y, w, h = roi
    prev = prev_gray[y:y + h, x:x + w]
    curr = gray[y:y + h, x:x + w]
    if prev.size == 0 or curr.size == 0:
        return 0.0
    flow = cv2.calcOpticalFlowFarneback(prev, curr, None, 0.5, 2, 15, 3, 5, 1.2, 0)
    mag, _ = cv2.cartToPolar(flow[..., 0], flow[..., 1])
    return float(np.mean(mag)) / 20.0


def _device_roi(width, height):
    roi = _config_value("RECOGNITION", "DEVICE_ROI", None)
    if isinstance(roi, dict):
        x = max(0, min(int(roi.get("x", 0)), width - 1))
        y = max(0, min(int(roi.get("y", 0)), height - 1))
        w = max(1, min(int(roi.get("w", width - x)), width - x))
        h = max(1, min(int(roi.get("h", height - y)), height - y))
        return x, y, w, h
    return int(width * 0.55), int(height * 0.25), max(1, int(width * 0.35)), max(1, int(height * 0.45))


def _vibration_level(score):
    danger = float(_config_value("RECOGNITION", "VIBRATION_DANGER_THRESHOLD", 0.18))
    warning = float(_config_value("RECOGNITION", "VIBRATION_WARNING_THRESHOLD", 0.08))
    if score >= danger:
        return "danger"
    if score >= warning:
        return "warning"
    return "normal"


def _dedupe_boxes(boxes):
    result = []
    for box in boxes:
        if not any(_iou(box, existing) > 0.4 for existing in result):
            result.append(box)
    return result


def _iou(a, b):
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    ix1, iy1 = max(ax, bx), max(ay, by)
    ix2, iy2 = min(ax + aw, bx + bw), min(ay + ah, by + bh)
    inter = max(0, ix2 - ix1) * max(0, iy2 - iy1)
    union = aw * ah + bw * bh - inter
    return inter / union if union else 0


def _download_url(url, target):
    parsed = urlparse(url)
    if parsed.scheme in ("http", "https"):
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            with open(target, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        fh.write(chunk)
        return
    raise ValueError("unsupported video url: %s" % url)


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


def _upload_to_minio(local_path, bucket, object_name):
    client = _minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, str(local_path))
    return "%s/%s/%s" % (_minio_public_base_url().rstrip("/"), bucket, object_name)


def _minio_client():
    endpoint = _config_value("MINIO", "ENDPOINT", os.getenv("MINIO_ENDPOINT", "10.21.221.12:9000"))
    access_key = _config_value("MINIO", "ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    secret_key = _config_value("MINIO", "SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "Admin@hd2019"))
    secure = str(_config_value("MINIO", "SECURE", os.getenv("MINIO_SECURE", "false"))).lower() == "true"
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _minio_public_base_url():
    public_url = _config_value("MINIO", "PUBLIC_URL", os.getenv("MINIO_PUBLIC_URL", None))
    if public_url:
        return public_url
    endpoint = _config_value("MINIO", "ENDPOINT", os.getenv("MINIO_ENDPOINT", "10.21.221.12:9000"))
    secure = str(_config_value("MINIO", "SECURE", os.getenv("MINIO_SECURE", "false"))).lower() == "true"
    return "%s://%s" % ("https" if secure else "http", endpoint)


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


def _set_redis_status(monitor_id, status):
    try:
        import functions

        conn = functions.getRedisConn()
        conn.set("monitor:%s:recognition_status" % monitor_id, status, ex=86400)
        functions.releaseRedisConn(conn)
    except Exception:
        pass


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "recognition_result_topic": topic, "status": status}}


def _safe_unlink(path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass
