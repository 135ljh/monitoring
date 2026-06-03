#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime as dt
import json
import os
import tempfile
import threading
import time
import traceback
import uuid
from pathlib import Path
from urllib.parse import urlparse

import cv2
from kafka import KafkaProducer
from minio import Minio


RAW_VIDEO_TOPIC = "workshop.raw_video"
DEFAULT_CAMERA_ID = "CAM_001"
DEFAULT_SEGMENT_SECONDS = 10
DEFAULT_RECONNECT_SECONDS = 5

_TASKS = {}
_TASKS_LOCK = threading.Lock()


def start_capture(data):
    """
    start_capture接口:
    接入 RTSP/HTTP 摄像头，后台持续切分视频片段，上传到 MinIO，并向 Kafka
    topic workshop.raw_video 发布原始视频元数据。

    data:
        url: str 摄像头视频流地址，支持 RTSP、HTTP 等协议
        camera_id: str 可选，摄像头编号，默认 CAM_001
        monitor_id: str 可选，监测任务编号，不传则自动生成
        segment_seconds: int 可选，切片时长，默认 10 秒
    """
    source_url = (data or {}).get("url")
    if not source_url:
        raise ValueError("url is required")

    _validate_video_url(source_url)

    monitor_id = (data or {}).get("monitor_id") or _new_monitor_id()
    camera_id = (data or {}).get("camera_id") or DEFAULT_CAMERA_ID
    segment_seconds = int((data or {}).get("segment_seconds") or _config_value(
        "VIDEO_CAPTURE", "SEGMENT_SECONDS", default=DEFAULT_SEGMENT_SECONDS))
    raw_video_topic = _config_value("KAFKA", "RAW_VIDEO_TOPIC", default=RAW_VIDEO_TOPIC)

    with _TASKS_LOCK:
        task = _TASKS.get(monitor_id)
        if task and task.get("status") == "running":
            return {
                "msg": "success",
                "data": {
                    "monitor_id": monitor_id,
                    "camera_id": task["camera_id"],
                    "raw_video_topic": task["raw_video_topic"],
                    "status": "running"
                }
            }

        stop_event = threading.Event()
        task = {
            "monitor_id": monitor_id,
            "camera_id": camera_id,
            "url": source_url,
            "raw_video_topic": raw_video_topic,
            "segment_seconds": segment_seconds,
            "status": "starting",
            "started_at": _now_text(),
            "last_error": None,
            "stop_event": stop_event,
        }
        _TASKS[monitor_id] = task

        thread = threading.Thread(
            target=_capture_loop,
            args=(task,),
            name="video-capture-%s" % monitor_id,
            daemon=True,
        )
        task["thread"] = thread
        thread.start()

    _set_redis_status(monitor_id, "running")

    return {
        "msg": "success",
        "data": {
            "monitor_id": monitor_id,
            "camera_id": camera_id,
            "raw_video_topic": raw_video_topic,
            "status": "running"
        }
    }


def stop_capture(data):
    """
    stop_capture接口:
    停止指定 monitor_id 的后台采集任务；不传 monitor_id 时停止当前组件内全部采集任务。
    """
    monitor_id = (data or {}).get("monitor_id")
    stopped = []

    with _TASKS_LOCK:
        targets = [monitor_id] if monitor_id else list(_TASKS.keys())
        for item in targets:
            task = _TASKS.get(item)
            if not task:
                continue
            task["stop_event"].set()
            task["status"] = "stopping"
            stopped.append(item)

    for item in stopped:
        _set_redis_status(item, "stopping")

    return {
        "msg": "success",
        "data": {
            "monitor_id": monitor_id,
            "stopped_monitor_ids": stopped,
            "status": "stopping" if stopped else "not_found"
        }
    }


def _capture_loop(task):
    producer = None
    sequence = 1
    try:
        producer = _create_kafka_producer()
        _update_task(task["monitor_id"], status="running")

        while not task["stop_event"].is_set():
            capture = _open_capture(task["url"])
            if not capture.isOpened():
                message = "cannot open video stream, retrying: %s" % task["url"]
                print(message)
                _update_task(task["monitor_id"], status="reconnecting", last_error=message)
                if task["stop_event"].wait(int(_config_value(
                    "VIDEO_CAPTURE",
                    "RECONNECT_SECONDS",
                    default=DEFAULT_RECONNECT_SECONDS,
                ))):
                    break
                continue

            try:
                _update_task(task["monitor_id"], status="running", last_error=None)
                while not task["stop_event"].is_set():
                    clip = _record_clip(
                        capture=capture,
                        monitor_id=task["monitor_id"],
                        camera_id=task["camera_id"],
                        sequence=sequence,
                        segment_seconds=task["segment_seconds"],
                    )
                    if not clip:
                        break

                    msg = _upload_clip_and_build_message(task, clip, sequence)
                    producer.send(task["raw_video_topic"], msg)
                    producer.flush(timeout=10)
                    print("published raw video message: %s" % json.dumps(msg, ensure_ascii=False))
                    sequence += 1
            finally:
                capture.release()

            if task["stop_event"].wait(2):
                break
    except Exception:
        error = traceback.format_exc()
        print("video capture task failed:\n%s" % error)
        _update_task(task["monitor_id"], status="error", last_error=error)
        _set_redis_status(task["monitor_id"], "error")
    finally:
        if producer is not None:
            producer.close()
        final_status = "stopped" if task["stop_event"].is_set() else task.get("status", "stopped")
        _update_task(task["monitor_id"], status=final_status)
        _set_redis_status(task["monitor_id"], final_status)


def _open_capture(source_url):
    os.environ.setdefault(
        "OPENCV_FFMPEG_CAPTURE_OPTIONS",
        "rtsp_transport;tcp|stimeout;5000000",
    )
    params = [
        cv2.CAP_PROP_OPEN_TIMEOUT_MSEC,
        5000,
        cv2.CAP_PROP_READ_TIMEOUT_MSEC,
        5000,
    ]
    try:
        capture = cv2.VideoCapture(source_url, cv2.CAP_FFMPEG, params)
    except Exception:
        capture = cv2.VideoCapture(source_url, cv2.CAP_FFMPEG)
    if not capture.isOpened():
        capture = cv2.VideoCapture(source_url)
    return capture


def _record_clip(capture, monitor_id, camera_id, sequence, segment_seconds):
    fps = capture.get(cv2.CAP_PROP_FPS)
    if not fps or fps <= 1 or fps > 120:
        fps = 25.0

    width = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    if width <= 0 or height <= 0:
        ok, frame = capture.read()
        if not ok or frame is None:
            return None
        height, width = frame.shape[:2]
        first_frame = frame
    else:
        first_frame = None

    start = dt.datetime.now()
    temp_dir = Path(tempfile.gettempdir()) / "workshop_video_capture" / monitor_id
    temp_dir.mkdir(parents=True, exist_ok=True)

    clip_id = "clip_%s_%06d" % (monitor_id, sequence)
    video_path = temp_dir / ("%s.mp4" % clip_id)
    cover_path = temp_dir / ("%s.jpg" % clip_id)

    writer = cv2.VideoWriter(
        str(video_path),
        cv2.VideoWriter_fourcc(*"mp4v"),
        fps,
        (width, height),
    )
    if not writer.isOpened():
        raise RuntimeError("cannot create video writer: %s" % video_path)

    frames = 0
    try:
        if first_frame is not None:
            writer.write(first_frame)
            cv2.imwrite(str(cover_path), first_frame)
            frames += 1

        deadline = time.time() + segment_seconds
        while time.time() < deadline:
            ok, frame = capture.read()
            if not ok or frame is None:
                break
            if frames == 0:
                cv2.imwrite(str(cover_path), frame)
            writer.write(frame)
            frames += 1
    finally:
        writer.release()

    if frames == 0:
        _safe_unlink(video_path)
        _safe_unlink(cover_path)
        return None

    end = dt.datetime.now()
    return {
        "clip_id": clip_id,
        "video_path": video_path,
        "cover_path": cover_path,
        "start_time": start,
        "end_time": end,
        "duration": round((end - start).total_seconds(), 3),
    }


def _upload_clip_and_build_message(task, clip, sequence):
    bucket = _config_value("MINIO", "BUCKET", default="workshop")
    prefix = _config_value("MINIO", "PREFIX", default="raw")
    object_prefix = "%s/%s/%s" % (prefix.strip("/"), task["camera_id"], task["monitor_id"])

    raw_video_url = _upload_to_minio(
        clip["video_path"],
        bucket,
        "%s/%s.mp4" % (object_prefix, clip["clip_id"]),
    )
    cover_frame_url = _upload_to_minio(
        clip["cover_path"],
        bucket,
        "%s/%s.jpg" % (object_prefix, clip["clip_id"]),
    )

    _safe_unlink(clip["video_path"])
    _safe_unlink(clip["cover_path"])

    return {
        "monitor_id": task["monitor_id"],
        "camera_id": task["camera_id"],
        "clip_id": clip["clip_id"],
        "raw_video_url": raw_video_url,
        "cover_frame_url": cover_frame_url,
        "start_time": _format_dt(clip["start_time"]),
        "end_time": _format_dt(clip["end_time"]),
        "duration": clip["duration"],
        "sequence": sequence,
    }


def _create_kafka_producer():
    bootstrap_servers = _config_value(
        "KAFKA",
        "BOOTSTRAP_SERVERS",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    if isinstance(bootstrap_servers, str):
        bootstrap_servers = [item.strip() for item in bootstrap_servers.split(",") if item.strip()]

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        api_version_auto_timeout_ms=5000,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        retries=3,
        linger_ms=50,
    )


def _upload_to_minio(local_path, bucket, object_name):
    client = _create_minio_client()
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.fput_object(bucket, object_name, str(local_path))
    return "%s/%s/%s" % (_minio_public_base_url().rstrip("/"), bucket, object_name)


def _create_minio_client():
    endpoint = _config_value("MINIO", "ENDPOINT", default=os.getenv("MINIO_ENDPOINT", "10.21.221.12:9000"))
    access_key = _config_value("MINIO", "ACCESS_KEY", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    secret_key = _config_value("MINIO", "SECRET_KEY", default=os.getenv("MINIO_SECRET_KEY", "Admin@hd2019"))
    secure = bool(_config_value("MINIO", "SECURE", default=os.getenv("MINIO_SECURE", "false")).__str__().lower() == "true")
    return Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def _minio_public_base_url():
    public_url = _config_value("MINIO", "PUBLIC_URL", default=os.getenv("MINIO_PUBLIC_URL"))
    if public_url:
        return public_url
    endpoint = _config_value("MINIO", "ENDPOINT", default=os.getenv("MINIO_ENDPOINT", "10.21.221.12:9000"))
    secure = bool(_config_value("MINIO", "SECURE", default=os.getenv("MINIO_SECURE", "false")).__str__().lower() == "true")
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
        conn.set("monitor:%s:capture_status" % monitor_id, status, ex=86400)
        functions.releaseRedisConn(conn)
    except Exception:
        pass


def _update_task(monitor_id, **kwargs):
    with _TASKS_LOCK:
        if monitor_id in _TASKS:
            _TASKS[monitor_id].update(kwargs)


def _validate_video_url(source_url):
    parsed = urlparse(source_url)
    if parsed.scheme.lower() not in ("rtsp", "http", "https", "file"):
        raise ValueError("unsupported video url scheme: %s" % parsed.scheme)


def _new_monitor_id():
    return "mon_%s_%s" % (dt.datetime.now().strftime("%Y%m%d_%H%M%S"), uuid.uuid4().hex[:6])


def _now_text():
    return _format_dt(dt.datetime.now())


def _format_dt(value):
    return value.strftime("%Y-%m-%d %H:%M:%S")


def _safe_unlink(path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass
