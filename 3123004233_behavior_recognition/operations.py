#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import os
import subprocess
import tempfile
import threading
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse

import cv2
import numpy as np
import requests
from kafka import KafkaConsumer, KafkaProducer
from minio import Minio


PROCESSED_VIDEO_TOPIC = "workshop.processed_video"
RECOGNITION_RESULT_TOPIC = "workshop.recognition_result"
DEFAULT_GROUP = "workshop-behavior-recognition"
BODY_25 = {
    "nose": 0,
    "neck": 1,
    "right_shoulder": 2,
    "right_elbow": 3,
    "right_wrist": 4,
    "left_shoulder": 5,
    "left_elbow": 6,
    "left_wrist": 7,
    "mid_hip": 8,
    "right_hip": 9,
    "right_knee": 10,
    "right_ankle": 11,
    "left_hip": 12,
    "left_knee": 13,
    "left_ankle": 14,
}

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
        "scene_result": analysis.get("scene_result", {}),
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
    upper_motion_scores = []
    person_boxes = []
    sampled_person_boxes = []
    frame_index = 0

    device_roi = _device_roi(width, height)

    try:
        while ok and frame is not None:
            gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
            if prev_gray is not None:
                diff = cv2.absdiff(prev_gray, gray)
                motion_scores.append(float(np.mean(diff)) / 255.0)
                upper_motion_scores.append(float(np.mean(diff[:max(1, height // 2), :])) / 255.0)
                vibration_scores.append(_optical_flow_score(prev_gray, gray, device_roi))

            if frame_index % max(1, int(fps)) == 0:
                boxes, weights = hog.detectMultiScale(frame, winStride=(8, 8), padding=(8, 8), scale=1.05)
                accepted = []
                for box, weight in zip(boxes, weights):
                    if float(weight) >= 0.2:
                        accepted.append([int(v) for v in box])
                if accepted:
                    person_boxes.extend(accepted)
                    sampled_person_boxes.append(_dedupe_boxes(accepted))

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
    upper_motion_score = round(float(np.mean(upper_motion_scores)) if upper_motion_scores else 0.0, 4)
    action_type = "static" if movement_score < float(_config_value("RECOGNITION", "STATIC_MOTION_THRESHOLD", 0.018)) else "moving"
    vibration_level = _vibration_level(vibration_score)

    deduped_boxes = _dedupe_boxes(person_boxes)
    openpose_tracks = _run_openpose(source_path, width, height)
    if openpose_tracks:
        person_results = _openpose_person_results(openpose_tracks, width, height, movement_score, action_type)
        person_count = len(person_results)
        pose_backend = "openpose"
    else:
        person_results = _fallback_person_results(
            deduped_boxes, sampled_person_boxes, width, height, movement_score, upper_motion_score, action_type)
        person_count = len(deduped_boxes)
        pose_backend = "opencv_hog"

    device_results = [{
        "device_id": _config_value("RECOGNITION", "DEVICE_ID", "DEV_001"),
        "roi": {"x": device_roi[0], "y": device_roi[1], "w": device_roi[2], "h": device_roi[3]},
        "vibration_score": vibration_score,
        "vibration_level": vibration_level,
        "optical_flow_value": round(vibration_score * 100.0, 4),
    }]

    scene_result = {
        "person_count": person_count,
        "crowd_count": person_count,
        "movement_score": movement_score,
        "upper_motion_score": upper_motion_score,
        "pose_backend": pose_backend,
    }

    return {"person_results": person_results, "device_results": device_results, "scene_result": scene_result}


def _fallback_person_results(deduped_boxes, sampled_person_boxes, width, height, movement_score, upper_motion_score, action_type):
    person_results = []
    center_speed = _person_center_speed(sampled_person_boxes, width, height)
    for idx, box in enumerate(deduped_boxes[:10] or [[0, 0, width, height]]):
        x, y, w, h = box
        posture_type, posture_score = _posture_from_box(box, width, height)
        fall_suspected = _fall_suspected(box, width, height, posture_type, bool(deduped_boxes))
        running_suspected = center_speed >= float(_config_value("RECOGNITION", "RUNNING_SPEED_THRESHOLD", 0.22))
        help_suspected = _help_gesture_suspected(upper_motion_score, movement_score, posture_type)
        person_results.append({
            "person_id": "P%03d" % (idx + 1),
            "bbox": [x, y, x + w, y + h],
            "action_type": action_type,
            "movement_score": movement_score,
            "center_speed": round(center_speed, 4),
            "posture_type": posture_type,
            "posture_score": round(posture_score, 4),
            "fall_suspected": fall_suspected,
            "running_suspected": running_suspected,
            "help_gesture_suspected": help_suspected,
            "confidence": 0.65 if deduped_boxes else 0.35,
        })
    return person_results


def _run_openpose(source_path, width, height):
    if not _config_bool("OPENPOSE", "ENABLED", False):
        return []

    exe_path = _openpose_exe_path()
    if not exe_path or not exe_path.exists():
        message = "OpenPose executable not found: %s" % (exe_path or "")
        if _config_bool("OPENPOSE", "REQUIRE_OPENPOSE", False):
            raise RuntimeError(message)
        print(message)
        return []

    json_dir = Path(tempfile.mkdtemp(prefix="openpose_json_"))
    try:
        cmd = [
            str(exe_path),
            "--video", str(source_path),
            "--write_json", str(json_dir),
            "--display", "0",
            "--render_pose", "0",
            "--model_pose", str(_config_value("OPENPOSE", "MODEL_POSE", "BODY_25")),
            "--number_people_max", str(int(_config_value("OPENPOSE", "NUMBER_PEOPLE_MAX", 10))),
        ]
        model_folder = _openpose_model_folder(exe_path)
        if model_folder:
            cmd.extend(["--model_folder", str(model_folder)])

        timeout = int(_config_value("OPENPOSE", "TIMEOUT_SECONDS", 180))
        completed = subprocess.run(
            cmd,
            cwd=str(_openpose_root(exe_path)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
        )
        if completed.returncode != 0:
            detail = (completed.stderr or completed.stdout or "")[-1000:]
            if _config_bool("OPENPOSE", "REQUIRE_OPENPOSE", False):
                raise RuntimeError("OpenPose failed: %s" % detail)
            print("OpenPose failed: %s" % detail)
            return []

        return _parse_openpose_json(json_dir, width, height)
    except subprocess.TimeoutExpired:
        if _config_bool("OPENPOSE", "REQUIRE_OPENPOSE", False):
            raise
        print("OpenPose timed out for %s" % source_path)
        return []
    finally:
        _remove_tree(json_dir)


def _openpose_exe_path():
    configured = _config_value("OPENPOSE", "EXE_PATH", os.getenv("OPENPOSE_EXE_PATH"))
    if configured:
        return Path(str(configured))

    root = _config_value("OPENPOSE", "ROOT", os.getenv("OPENPOSE_ROOT"))
    if root:
        root_path = Path(str(root))
        candidates = [
            root_path / "bin" / "OpenPoseDemo.exe",
            root_path / "bin" / "openpose.exe",
            root_path / "build" / "x64" / "Release" / "OpenPoseDemo.exe",
            root_path / "OpenPoseDemo.exe",
        ]
        for item in candidates:
            if item.exists():
                return item
    return None


def _openpose_root(exe_path):
    root = _config_value("OPENPOSE", "ROOT", os.getenv("OPENPOSE_ROOT"))
    if root:
        return Path(str(root))
    if exe_path.parent.name.lower() == "bin":
        return exe_path.parent.parent
    return exe_path.parent


def _openpose_model_folder(exe_path):
    configured = _config_value("OPENPOSE", "MODEL_FOLDER", os.getenv("OPENPOSE_MODEL_FOLDER"))
    if configured:
        return Path(str(configured))
    root = _openpose_root(exe_path)
    candidate = root / "models"
    return candidate if candidate.exists() else None


def _parse_openpose_json(json_dir, width, height):
    tracks = []
    for json_file in sorted(json_dir.glob("*_keypoints.json")):
        with open(json_file, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        people = data.get("people", []) or []
        people = sorted(people, key=_openpose_person_score, reverse=True)
        for idx, person in enumerate(people[:int(_config_value("OPENPOSE", "NUMBER_PEOPLE_MAX", 10))]):
            while len(tracks) <= idx:
                tracks.append({"frames": []})
            keypoints = _openpose_keypoints(person)
            if keypoints is not None:
                tracks[idx]["frames"].append(keypoints)

    min_frames = int(_config_value("OPENPOSE", "MIN_VALID_FRAMES", 2))
    return [
        track for track in tracks
        if len(track.get("frames", [])) >= min_frames and _valid_openpose_track(track, width, height)
    ]


def _openpose_person_score(person):
    keypoints = _openpose_keypoints(person)
    if keypoints is None:
        return 0.0
    valid = keypoints[:, 2] > float(_config_value("OPENPOSE", "KEYPOINT_CONFIDENCE", 0.05))
    if not np.any(valid):
        return 0.0
    xs = keypoints[valid, 0]
    ys = keypoints[valid, 1]
    area = max(1.0, float((xs.max() - xs.min()) * (ys.max() - ys.min())))
    return area * float(np.mean(keypoints[valid, 2]))


def _openpose_keypoints(person):
    values = person.get("pose_keypoints_2d")
    if not values:
        return None
    arr = np.array(values, dtype=float).reshape((-1, 3))
    if arr.shape[0] < 15:
        return None
    return arr


def _openpose_person_results(tracks, width, height, movement_score, action_type):
    results = []
    for idx, track in enumerate(tracks[:int(_config_value("OPENPOSE", "NUMBER_PEOPLE_MAX", 10))]):
        frames = track.get("frames", [])
        latest = frames[-1]
        bbox = _keypoint_bbox(latest, width, height)
        center_speed = _keypoint_center_speed(frames, width, height)
        posture_type, posture_score = _posture_from_keypoints(frames, width, height)
        fall_suspected = _fall_from_keypoints(frames, width, height)
        help_suspected = _help_from_keypoints(frames, height)
        running_suspected = center_speed >= float(_config_value("RECOGNITION", "RUNNING_SPEED_THRESHOLD", 0.22))
        results.append({
            "person_id": "P%03d" % (idx + 1),
            "bbox": bbox,
            "action_type": action_type,
            "movement_score": movement_score,
            "center_speed": round(center_speed, 4),
            "posture_type": posture_type,
            "posture_score": round(posture_score, 4),
            "fall_suspected": fall_suspected,
            "running_suspected": running_suspected,
            "help_gesture_suspected": help_suspected,
            "keypoint_format": "BODY_25",
            "keypoint_backend": "openpose",
            "valid_keypoint_count": _valid_keypoint_count(latest),
            "track_frame_count": len(frames),
            "confidence": round(_mean_keypoint_confidence(latest), 4),
        })
    return results


def _valid_openpose_track(track, width, height):
    frames = track.get("frames", [])
    if not frames:
        return False
    latest = frames[-1]
    confidence = _mean_keypoint_confidence(latest)
    valid_count = _valid_keypoint_count(latest)
    x1, y1, x2, y2 = _keypoint_bbox(latest, width, height)
    bbox_w = max(0, x2 - x1)
    bbox_h = max(0, y2 - y1)
    area_ratio = (bbox_w * bbox_h) / float(max(1, width * height))
    height_ratio = bbox_h / float(max(1, height))
    min_conf = float(_config_value("OPENPOSE", "MIN_PERSON_CONFIDENCE", 0.35))
    min_points = int(_config_value("OPENPOSE", "MIN_VALID_KEYPOINTS", 6))
    min_area = float(_config_value("OPENPOSE", "MIN_BBOX_AREA_RATIO", 0.02))
    min_height = float(_config_value("OPENPOSE", "MIN_BBOX_HEIGHT_RATIO", 0.18))
    if confidence < min_conf or valid_count < min_points:
        return False
    if area_ratio < min_area or height_ratio < min_height:
        return False
    return _has_core_body_keypoints(latest)


def _keypoint_bbox(keypoints, width, height):
    valid = keypoints[:, 2] > float(_config_value("OPENPOSE", "KEYPOINT_CONFIDENCE", 0.05))
    if not np.any(valid):
        return [0, 0, width, height]
    xs = keypoints[valid, 0]
    ys = keypoints[valid, 1]
    return [
        int(max(0, np.min(xs))),
        int(max(0, np.min(ys))),
        int(min(width, np.max(xs))),
        int(min(height, np.max(ys))),
    ]


def _valid_keypoint_count(keypoints):
    return int(np.sum(keypoints[:, 2] > float(_config_value("OPENPOSE", "KEYPOINT_CONFIDENCE", 0.05))))


def _has_core_body_keypoints(keypoints):
    core_groups = [
        ("neck",),
        ("left_shoulder", "right_shoulder"),
        ("mid_hip", "left_hip", "right_hip"),
    ]
    for names in core_groups:
        if not any(_point(keypoints, name) is not None for name in names):
            return False
    return True


def _keypoint_center_speed(frames, width, height):
    centers = []
    for keypoints in frames:
        center = _person_center_from_keypoints(keypoints)
        if center is not None:
            centers.append((center[0] / max(1, width), center[1] / max(1, height)))
    if len(centers) < 2:
        return 0.0
    distances = []
    for idx in range(1, len(centers)):
        dx = centers[idx][0] - centers[idx - 1][0]
        dy = centers[idx][1] - centers[idx - 1][1]
        distances.append(float(np.sqrt(dx * dx + dy * dy)))
    return float(np.mean(distances)) if distances else 0.0


def _person_center_from_keypoints(keypoints):
    points = []
    for name in ("neck", "mid_hip", "left_hip", "right_hip"):
        point = _point(keypoints, name)
        if point is not None:
            points.append(point)
    if not points:
        return None
    arr = np.array(points, dtype=float)
    return float(np.mean(arr[:, 0])), float(np.mean(arr[:, 1]))


def _posture_from_keypoints(frames, width, height):
    keypoints = frames[-1]
    torso_angle = _torso_angle_from_horizontal(keypoints)
    knee_angle = _min_knee_angle(keypoints)
    shoulder_y = _avg_y(keypoints, ("left_shoulder", "right_shoulder", "neck"))
    hip_y = _avg_y(keypoints, ("left_hip", "right_hip", "mid_hip"))
    if torso_angle is not None and torso_angle <= float(_config_value("OPENPOSE", "FALL_TORSO_ANGLE", 35)):
        return "horizontal", 1.0 - torso_angle / 90.0
    if knee_angle is not None and knee_angle <= float(_config_value("OPENPOSE", "SQUAT_KNEE_ANGLE", 95)):
        return "squat", 1.0 - knee_angle / 180.0
    if shoulder_y is not None and hip_y is not None and abs(shoulder_y - hip_y) <= height * float(_config_value("OPENPOSE", "BEND_SHOULDER_HIP_DELTA_RATIO", 0.18)):
        return "bend", 1.0 - abs(shoulder_y - hip_y) / max(1.0, height)
    if torso_angle is not None and torso_angle <= float(_config_value("OPENPOSE", "BEND_TORSO_ANGLE", 60)):
        return "bend", 1.0 - torso_angle / 90.0
    return "standing", 0.0


def _fall_from_keypoints(frames, width, height):
    keypoints = frames[-1]
    torso_angle = _torso_angle_from_horizontal(keypoints)
    head = _point(keypoints, "nose") or _point(keypoints, "neck")
    hip = _point(keypoints, "mid_hip")
    if torso_angle is None or head is None:
        return False
    head_low = head[1] >= height * float(_config_value("OPENPOSE", "FALL_HEAD_LOW_RATIO", 0.45))
    hip_low = hip is not None and hip[1] >= height * float(_config_value("OPENPOSE", "FALL_HIP_LOW_RATIO", 0.50))
    return torso_angle <= float(_config_value("OPENPOSE", "FALL_TORSO_ANGLE", 35)) and (head_low or hip_low)


def _help_from_keypoints(frames, height):
    raised_frames = 0
    wrist_y_values = []
    for keypoints in frames:
        shoulder_y = _avg_y(keypoints, ("left_shoulder", "right_shoulder"))
        if shoulder_y is None:
            continue
        raised = False
        for wrist_name in ("left_wrist", "right_wrist"):
            wrist = _point(keypoints, wrist_name)
            if wrist is not None:
                wrist_y_values.append(wrist[1])
                if wrist[1] < shoulder_y:
                    raised = True
        if raised:
            raised_frames += 1
    if raised_frames < int(_config_value("OPENPOSE", "HELP_MIN_RAISED_FRAMES", 2)):
        return False
    if len(wrist_y_values) < 2:
        return True
    amplitude = max(wrist_y_values) - min(wrist_y_values)
    return amplitude >= height * float(_config_value("OPENPOSE", "HELP_WRIST_AMPLITUDE_RATIO", 0.08))


def _torso_angle_from_horizontal(keypoints):
    neck = _point(keypoints, "neck")
    hip = _point(keypoints, "mid_hip") or _avg_point(keypoints, ("left_hip", "right_hip"))
    if neck is None or hip is None:
        return None
    dx = abs(neck[0] - hip[0])
    dy = abs(neck[1] - hip[1])
    return float(np.degrees(np.arctan2(dy, max(dx, 1e-6))))


def _min_knee_angle(keypoints):
    angles = []
    for side in ("left", "right"):
        hip = _point(keypoints, "%s_hip" % side)
        knee = _point(keypoints, "%s_knee" % side)
        ankle = _point(keypoints, "%s_ankle" % side)
        angle = _angle(hip, knee, ankle)
        if angle is not None:
            angles.append(angle)
    return min(angles) if angles else None


def _angle(a, b, c):
    if a is None or b is None or c is None:
        return None
    ba = np.array([a[0] - b[0], a[1] - b[1]], dtype=float)
    bc = np.array([c[0] - b[0], c[1] - b[1]], dtype=float)
    denom = float(np.linalg.norm(ba) * np.linalg.norm(bc))
    if denom <= 1e-6:
        return None
    cosine = float(np.clip(np.dot(ba, bc) / denom, -1.0, 1.0))
    return float(np.degrees(np.arccos(cosine)))


def _point(keypoints, name):
    idx = BODY_25.get(name)
    if idx is None or idx >= len(keypoints):
        return None
    x, y, conf = keypoints[idx]
    if conf < float(_config_value("OPENPOSE", "KEYPOINT_CONFIDENCE", 0.05)):
        return None
    return float(x), float(y)


def _avg_point(keypoints, names):
    points = [_point(keypoints, name) for name in names]
    points = [point for point in points if point is not None]
    if not points:
        return None
    arr = np.array(points, dtype=float)
    return float(np.mean(arr[:, 0])), float(np.mean(arr[:, 1]))


def _avg_y(keypoints, names):
    point = _avg_point(keypoints, names)
    return point[1] if point is not None else None


def _mean_keypoint_confidence(keypoints):
    valid = keypoints[:, 2] > float(_config_value("OPENPOSE", "KEYPOINT_CONFIDENCE", 0.05))
    if not np.any(valid):
        return 0.0
    return float(np.mean(keypoints[valid, 2]))


def _person_center_speed(sampled_boxes, width, height):
    centers = []
    for boxes in sampled_boxes:
        if not boxes:
            continue
        x, y, w, h = max(boxes, key=lambda item: item[2] * item[3])
        centers.append(((x + w / 2.0) / max(1, width), (y + h / 2.0) / max(1, height)))
    if len(centers) < 2:
        return 0.0
    distances = []
    for idx in range(1, len(centers)):
        dx = centers[idx][0] - centers[idx - 1][0]
        dy = centers[idx][1] - centers[idx - 1][1]
        distances.append(float(np.sqrt(dx * dx + dy * dy)))
    return float(np.mean(distances)) if distances else 0.0


def _posture_from_box(box, width, height):
    _, y, w, h = box
    aspect = float(w) / max(1.0, float(h))
    height_ratio = float(h) / max(1.0, float(height))
    center_y = (float(y) + h / 2.0) / max(1.0, float(height))

    if aspect >= float(_config_value("RECOGNITION", "FALL_ASPECT_THRESHOLD", 1.15)):
        return "horizontal", aspect
    if height_ratio <= float(_config_value("RECOGNITION", "SQUAT_HEIGHT_RATIO", 0.42)) and center_y > 0.45:
        return "squat", height_ratio
    if aspect >= float(_config_value("RECOGNITION", "BEND_ASPECT_THRESHOLD", 0.72)):
        return "bend", aspect
    return "standing", height_ratio


def _fall_suspected(box, width, height, posture_type, detected):
    if not detected:
        return False
    _, y, w, h = box
    aspect = float(w) / max(1.0, float(h))
    bottom_ratio = float(y + h) / max(1.0, float(height))
    return posture_type == "horizontal" and aspect >= 1.15 and bottom_ratio >= 0.55


def _help_gesture_suspected(upper_motion_score, movement_score, posture_type):
    if posture_type not in ("standing", "bend"):
        return False
    threshold = float(_config_value("RECOGNITION", "HELP_GESTURE_UPPER_MOTION_THRESHOLD", 0.08))
    return upper_motion_score >= threshold and upper_motion_score >= movement_score * 1.4


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
        try:
            with requests.get(url, stream=True, timeout=30) as resp:
                resp.raise_for_status()
                with open(target, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)
            return
        except requests.HTTPError as exc:
            if exc.response is None or exc.response.status_code != 403:
                raise
            _download_from_minio_url(url, target)
            return
    raise ValueError("unsupported video url: %s" % url)


def _download_from_minio_url(url, target):
    parsed = urlparse(url)
    parts = [unquote(item) for item in parsed.path.strip("/").split("/") if item]
    if len(parts) < 2:
        raise ValueError("invalid MinIO object url: %s" % url)
    bucket = parts[0]
    object_name = "/".join(parts[1:])
    _minio_client().fget_object(bucket, object_name, str(target))


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


def _config_bool(section, key, default):
    value = _config_value(section, key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


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


def _remove_tree(path):
    path = Path(path)
    if not path.exists():
        return
    for child in path.iterdir():
        if child.is_dir():
            _remove_tree(child)
        else:
            _safe_unlink(child)
    try:
        path.rmdir()
    except Exception:
        pass
