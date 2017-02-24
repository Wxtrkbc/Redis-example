# -*- coding: utf-8 -*-
import time
import json

QUIT = False
LIMIT = 10000000


def check_token(conn, token):
    return conn.hget('login:', token)


def update_token(conn, token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)
    if item:
        conn.zadd('viewed:' + token, item, timestamp)
        conn.zremrangebyrank('viewed:' + token, 0, -26)  # 根据范围进行删除 只保留最近浏览的25个商品
        conn.zincrby('viewed:', item, -1)  # 分值最少浏览次数越多


def add_to_cart(conn, session, item, count):
    if count <= 0:
        conn.hrem('cart:' + session, item)
    else:
        conn.hset('cart:' + session, item, count)


# 保持最新的1000万个会话
def clean_session(conn):
    while not QUIT:  # 守护进程的方式运行， 或者定时任务
        size = conn.zcard('recent:')   # 获取'recent:'对应的有序集合元素的数量
        if size <= LIMIT:
            time.sleep(1)
            continue

        end_index = min(size-LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index-1)  # 获取要移除的id集合

        session_keys = []
        for session in sessions:
            session_keys.append('viewed:' + session)
            session_keys.append('cart:' + session)

        conn.delete(*session_keys)
        conn.hdel('login:', *sessions)
        conn.zrem('recent:', *sessions)


def schedule_row_cache(conn, row_id, delay):
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:', row_id, time.time())


def cache_rows(conn):
    while not QUIT:
        # 获取一个需要被缓存的数据行的分值 命令返回一个包含零个或1个元组的列表
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(0.05)
            continue
        row_id = next[0][0]
        delay = conn.zscroe('delay:', row_id)  # 获取相对应的分数

        if delay <= 0:
            conn.zrem('delay:', row_id)
            conn.zrem('schedule:', row_id)
            conn.delete('inv:' + row_id)

        # row = Inventory.get(row_id)  # E
        row = {'xx': 'xx'}
        conn.zadd('schedule:', row_id, now + delay)  # F
        conn.set('inv:' + row_id, json.dumps(row))


def rescale_viewed(conn):
    while not QUIT:
        conn.zremrangebyrank('viewed:', 0, -20001)
        conn.zinterstore('viewed:', {'viewed:': .5})  # 将浏览次数降低为原来一半
        time.sleep(300)
