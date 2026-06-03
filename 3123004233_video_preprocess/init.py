#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import sys
import os

import routes
import server


class Config(object):
    def __init__(self, server_id, device_ip):
        self.SERVER_ID = server_id
        self.DEVICE_IP = device_ip
        self.SUB_TOPIC = "e/req/" + server_id
        self.PUB_TOPIC = "e/resp/" + server_id


def _parse_env(s):
    if s is None:
        return None
    s = s.strip()
    if not s:
        return None
    if (s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"'):
        s = s[1:-1]
    try:
        return json.loads(s)
    except Exception:
        return None

envStr = sys.argv[1] if len(sys.argv) > 1 else ""
env = _parse_env(envStr) or _parse_env(os.getenv("ISDP_JSON")) or _parse_env(os.getenv("JSON_PARAMS"))
if env is None:
    raise Exception("启动参数解析失败: 无效的 JSON")
if env.get("isdp.instance-id") is None:
    raise Exception("Can't get isdp.instance-id")
cfg = Config(server_id=env.get("isdp.instance-id"), device_ip=env.get("isdp.instance-ip", env.get("isdp.instance-id")))

if __name__ == '__main__':
    print("project: ", '3123004233_video_preprocess')
    print("启动参数: ", envStr)
    addr = env.get("isdp.nacos", "http://nacos:8848")

    client = server.init(cfg, addr, "component")

    server.load(client, cfg)

    routes.refresh(cfg)

    server.init_mqtt()

