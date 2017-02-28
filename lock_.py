# coding=utf-8
import uuid
import time
import redis
import math


def acquire_lock(conn, lockname, acquire_timeout=10):
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx('lock:' + lockname, identifier):  # 命令在指定的key不存在时，为key设置指定的值
            return identifier
        time.sleep(.001)
    return False


def release_lock(conn, lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname
    while True:
        try:
            pipe.watech(lockname)
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False


def purchase_item_with_lock(conn, buyerid, itemid, sellerid):
    buyer = "users:{}".format(buyerid)
    seller = "users:{}".format(sellerid)
    item = "{}.{}".format(itemid, sellerid)
    inventory = "inventory:{}".format(buyerid)

    locked = acquire_lock(conn, 'market:')
    if not locked:
        return False

    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.execute()
        if price is None or price > funds:
            return None

        pipe.hincrby(seller, "funds", int(price))
        pipe.hincrby(buyer, "funds", int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem("market:", item)
        pipe.execute()
        return True
    finally:
        release_lock(conn, 'market:', locked)


def acquire_lock_with_timeout(conn, lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))  # 函数返回数字的上入整数

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):  # 获取锁，并设置过期时间
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):  # 以秒为单位返回 key 的剩余过期时间
            conn.expire(lockname, lock_timeout)
        time.sleep(.001)
    return False
