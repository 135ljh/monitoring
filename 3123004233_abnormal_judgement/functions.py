#!/usr/bin/env python
# -*- coding: utf-8 -*-
import server


# 请按如下方式调用redis：
#     conn = functions.getRedisConn()
#     data = conn.get(key)
#     functions.releaseRedisConn(conn)

def getRedisConn():
    return server.redisConn()


def releaseRedisConn(conn):
    conn.close()
