#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from urllib.parse import urlparse

import cv2
import requests
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio


RAW_VIDEO_TOPIC = "workshop.raw_video"
PROCESSED_VIDEO_TOPIC = "workshop.processed_video"
DEFAULT_GROUP = "workshop-video-preprocess"

_TASKS = {}
_LOCK = threading.Lock()


def start_preprocess(data):
    monitor_id = (data or {}).get("monitor_id")
    if not monitor_id:
        raise ValueError("monitor_id is required")

    raw_topic = (data or {}).get("raw_video_topic") or _config_value("KAFKA", "RAW_VIDEO_TOPIC", RAW_VIDEO_TOPIC)
    processed_topic = _config_value("KAFKA", "PROCESSED_VIDEO_TOPIC", PROCESSED_VIDEO_TOPIC)

    with _LOCK:
        task = _TASKS.get(monitor_id)
        if task and task.get("status") == "running":
            return _response(monitor_id, processed_topic, "running")

        task = {
            "monitor_id": monitor_id,
            "raw_topic": raw_topic,
            "processed_topic": processed_topic,
            "status": "running",
            "stop_event": threading.Event(),
        }
        _TASKS[monitor_id] = task
        thread = threading.Thread(target=_consume_loop, args=(task,), daemon=True, name="video-preprocess-%s" % monitor_id)
        task["thread"] = thread
        thread.start()

    _set_redis_status(monitor_id, "running")
    return _response(monitor_id, processed_topic, "running")


def _consume_loop(task):
    consumer = None
    producer = None
    try:
        consumer = _create_consumer(task["raw_topic"], DEFAULT_GROUP + "-" + task["monitor_id"])
        producer = _create_producer()
        for record in consumer:
            if task["stop_event"].is_set():
                break
            msg = record.value
            if msg.get("monitor_id") != task["monitor_id"]:
                continue
            try:
                processed = _process_message(msg)
                producer.send(task["processed_topic"], processed)
                producer.flush(timeout=10)
                print("published processed video message: %s" % json.dumps(processed, ensure_ascii=False))
            except Exception as exc:
                print("preprocess failed: %s" % exc)
    except Exception as exc:
        print("video preprocess task failed: %s" % exc)
        _update_task(task["monitor_id"], status="error", last_error=str(exc))
        _set_redis_status(task["monitor_id"], "error")
    finally:
        if consumer:
            consumer.close()
        if producer:
            producer.close()


def _process_message(msg):
    monitor_id = msg["monitor_id"]
    camera_id = msg.get("camera_id", "CAM_001")
    clip_id = msg["clip_id"]
    temp_dir = Path(tempfile.gettempdir()) / "workshop_video_preprocess" / monitor_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    source_path = temp_dir / ("%s_raw.mp4" % clip_id)
    processed_path = temp_dir / ("%s_processed.mp4" % clip_id)
    frame_path = temp_dir / ("%s_processed.jpg" % clip_id)

    _download_url(msg["raw_video_url"], source_path)
    roi = _configured_roi()
    effective_roi = _preprocess_video(source_path, processed_path, frame_path, roi)

    bucket = _config_value("MINIO", "BUCKET", "workshop")
    prefix = _config_value("MINIO", "PREFIX", "processed").strip("/")
    object_prefix = "%s/%s/%s" % (prefix, camera_id, monitor_id)
    processed_video_url = _upload_to_minio(processed_path, bucket, "%s/%s.mp4" % (object_prefix, clip_id))
    processed_frame_url = _upload_to_minio(frame_path, bucket, "%s/%s.jpg" % (object_prefix, clip_id))

    _safe_unlink(source_path)
    _safe_unlink(processed_path)
    _safe_unlink(frame_path)

    return {
        "monitor_id": monitor_id,
        "camera_id": camera_id,
        "clip_id": clip_id,
        "processed_video_url": processed_video_url,
        "processed_frame_url": processed_frame_url,
        "original_video_url": msg["raw_video_url"],
        "roi": effective_roi,
        "start_time": msg.get("start_time"),
        "end_time": msg.get("end_time"),
        "sequence": msg.get("sequence"),
    }


def _preprocess_video(source_path, output_path, frame_path, roi):
    cap = cv2.VideoCapture(str(source_path))
    if not cap.isOpened():
        raise RuntimeError("cannot open raw video: %s" % source_path)

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    ok, frame = cap.read()
    if not ok or frame is None:
        cap.release()
        raise RuntimeError("raw video has no frames: %s" % source_path)

    cropped, effective_roi = _crop(frame, roi)
    height, width = cropped.shape[:2]
    writer = cv2.VideoWriter(str(output_path), cv2.VideoWriter_fourcc(*"mp4v"), fps, (width, height))
    if not writer.isOpened():
        cap.release()
        raise RuntimeError("cannot create processed video: %s" % output_path)

    try:
        while ok and frame is not None:
            cropped, _ = _crop(frame, roi)
            denoised = cv2.GaussianBlur(cropped, (3, 3), 0)
            writer.write(denoised)
            if not frame_path.exists():
                cv2.imwrite(str(frame_path), denoised)
            ok, frame = cap.read()
    finally:
        writer.release()
        cap.release()

    return effective_roi


def _crop(frame, roi):
    h, w = frame.shape[:2]
    if not roi:
        return frame, {"x": 0, "y": 0, "w": w, "h": h}

    x = max(0, min(int(roi.get("x", 0)), w - 1))
    y = max(0, min(int(roi.get("y", 0)), h - 1))
    rw = max(1, min(int(roi.get("w", w - x)), w - x))
    rh = max(1, min(int(roi.get("h", h - y)), h - y))
    return frame[y:y + rh, x:x + rw], {"x": x, "y": y, "w": rw, "h": rh}


def _configured_roi():
    value = _config_value("VIDEO_PREPROCESS", "ROI", None)
    return value if isinstance(value, dict) else None


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
        conn.set("monitor:%s:preprocess_status" % monitor_id, status, ex=86400)
        functions.releaseRedisConn(conn)
    except Exception:
        pass


def _update_task(monitor_id, **kwargs):
    with _LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _response(monitor_id, topic, status):
    return {"msg": "success", "data": {"monitor_id": monitor_id, "processed_video_topic": topic, "status": status}}


def _safe_unlink(path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass
