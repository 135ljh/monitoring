#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import json
import time
import traceback
from threading import Thread

import nacos
import paho.mqtt.client as mqtt
import redis
import yaml

import routes

config = None
REDIS_POOL = None


def redisConn():
    global REDIS_POOL
    if REDIS_POOL is None:
        cfg = config.get("REDIS")
        REDIS_POOL = redis.ConnectionPool(
            host=cfg.get("HOST"),
            port=cfg.get("PORT"),
            password=cfg.get("PASSWORD"),
            db=cfg.get("DB"),
            max_connections=50)
        print("redis连接池初始化完成")
    return redis.Redis(connection_pool=REDIS_POOL)



# no auth mode
def init(cfg, address, namespace):
    client = nacos.NacosClient(server_addresses=address, namespace=namespace)
    register(client, service_name=cfg.SERVER_ID, ip=cfg.DEVICE_IP, port=80)
    return client


def load(client, cfg):
    c1 = client.get_config(data_id="common.yml", group="DEFAULT_GROUP")
    print("云公共配置: ", c1)
    c2 = client.get_config(data_id=cfg.SERVER_ID + ".yml", group="PYTHON")
    print("云私有配置: ", c2)
    global config
    config = cfg.__dict__
    if c1 is not None:
        config.update(yaml.safe_load(c1))
    if c2 is not None:
        config.update(yaml.safe_load(c2))
    print("整体配置", config)


def register(client, service_name, ip, port):
    client.add_naming_instance(service_name, ip, port, group_name="PYTHON")
    Thread(target=beat, args=[client, service_name, ip, port]).start()


def beat(client, service_name, ip, port):
    while True:
        client.send_heartbeat(service_name, ip, port, group_name="PYTHON")
        # print(str(datetime.datetime.now()) + " Nacos心跳:", service_name, ip, port)
        time.sleep(5)


def init_mqtt():
    create_mqtt_client().loop_forever()


def create_mqtt_client():
    print("服务实例：" + config["SERVER_ID"])
    print("IP地址：" + config["DEVICE_IP"])
    client = mqtt.Client(client_id=config["SERVER_ID"])
    client.username_pw_set(config["MQTT"]["USERNAME"], config["MQTT"]["PASSWORD"])
    client.connect(config["MQTT"]["BROKER"], config["MQTT"]["PORT"], 60)
    client.on_connect = on_connect
    client.on_message = on_message
    return client


def on_connect(client, userdata, flags, rc):
    print("mqtt连接状态码：" + str(rc))
    client.subscribe(config["SUB_TOPIC"] + "/#")
    print("订阅主题:")
    for route in routes.list:
        print(" · " + route.recv_topic)


def on_message(client, userdata, msg):
    topic = msg.topic

    if not msg.payload:
        print("收到空报文，主题:", topic)
        return

    body = msg.payload.decode()
    print(">>>>>>\n" + str(datetime.datetime.now()) + "  收到请求")
    print(topic + " \n" + body)

    req = json.loads(body)
    data = req["data"]
    resp = {}

    for route in routes.list:
        if route.recv_topic == topic:
            try:
                output = route.func(data)

                resp["code"] = 200
                resp["msg"] = output.get("msg", "success")
                resp["data"] = output.get("data", {})

                resp["headers"] = req["headers"]

            except Exception as e:
                # 打印异常信息
                error = "error：" + traceback.format_exc()
                print(error)
                resp["code"] = 500
                resp["msg"] = error
                resp["headers"] = req["headers"]
            finally:

                send_topic = route.send_topic
                payload_json = json.dumps(resp, ensure_ascii=False)
                if resp["code"] == 200:
                    print("响应报文:\n" + send_topic + " \n" + payload_json)
                client.publish(send_topic, payload=payload_json, qos=0, retain=False)
                return
