# coding=utf-8
import logging
import time
from datetime import datetime

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical',
}

SEVERITY.update([(name, name) for name in SEVERITY.values()])
PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]
QUIT = False


"""
 known:   zset
 1:hits   0
 5:hits   0



 count:5:hits   hash

 1336375839  45
 1336375835  23

"""


def log_recent(conn, name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "recent:{}:{}".format(name, severity)
    message = time.asctime() + ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    pipe.ltrim(destination, 0, 99)  # 不在范围内的元素被清除
    pipe.execute()


def log_common(conn, name, message, severity=logging.INFO, timeout=5):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = "common:{}:{}".format(name, severity)
    start_key = destination + ':start'  # 纪录当前所处的小时数
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()  # 2017-02-26T10:00:00
            existing = pipe.get(start_key)
            pipe.multi()
            if existing and existing < hour_start:  # 如果是上一个小时的日志
                pipe.rename(destination, destination + ':start')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, message)
            log_recent(conn, name, message, severity, pipe)
            return
        except redis.exceptions.WatchError:
            continue


def update_counter(conn, name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now/prec) * prec
        hash = "{}:{}".format(prec, name)
        pope.zadd('known:', hash, 0)
        pipe.hincrby('count:' + hash, pnow, count)
    pipe.execute()


def get_counter(conn, name, precision):
    hash = "{}:{}".format(precision, name)
    data = conn.hgetall('count:' + hash)
    to_return = []
    for key, value in data.values():
        to_return.append((int(key), int(value)))
        to_return.sort()
    return to_return


def clean_counters(conn):
    pipe = conn.pipeline(True)
    passes = 0
    while not QUIT:
        start = time.time()
        index = 0
        while index < conn.zcard('known:'):   # 获取有序集合的数量
            hash = conn.zrange('known:', index, index)
            index += 1
            if not hash:
                break
            hash = hash[0]
            prec = int(hash.partition(":")[0])
            bprec = int(prec // 60) or 1
            if passes % bprec:
                continue

            hkey = 'count:' + hash
            cutoff = time.time() - SAMPLE_COUNT * prec
            samples = map(int, conn.hkeys(hkey))
            samples.sort()
            remove = bisect.bisect_right(samples, cutoff)

            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', hash)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass

        passes += 1
        duration = min(int(time.time() - start) + 1, 60)
        time.sleep(max(60 - duration, 1))

