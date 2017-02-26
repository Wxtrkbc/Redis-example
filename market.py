# coding=utf-8
import time


def list_item(conn, itemid, sellerid, price):
    inventory = "inventory:{}".format(sellerid)
    item = "{}.{}".format(itemid, sellerid)
    end = time.time() + 5
    pipe = conn.pipeline()
    while time.time() < end:
        try:
            pipe.watch(inventory) # 监视用户包裹发生的变化
            if not pipe.sismember(inventory, itemed):
                pipe.unwatch()
                return None

            pipe.multi()  #  开始新的事物
            pipe.zadd("market:", item, price)
            pipe.srem(inventory, itemid)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
        return False


def purchase_item(conn, buyerid, itemid, sellerid, lprice):
    buyer = "users:{}".format(buyerid)
    seller = "users:{}".format(sellerid)
    item = "{}.{}".format(itemid, sellerid)
    inventory = "inventory:{}".format(buyerid)
    end = conn.pipeline()

    while time.time() < end:
        try:
            pipe.watch("market:", buyer)
            price = pipe.zscore("market:", item)
            funds = int(pipe.hget(buyer, "funds"))
            if price != lprice or price > funds:
                pipe.unwatch()
                return None

            pipe.multi()
            pipe.hincrby(seller, "funds", int(price))
            pipe.hincrby(buyer, "funds", int(-price))
            pipe.sadd(inventory, itemid)
            pipe.zrem("market:", item)
            pipe.execute()
            return True
        except redis.exceptions.WatchError:
            pass
        return False


# 非事物型流水线
def update_token_pipeline(conn, token, user, item=None):
    timestamp = time.time()
    pipe = conn.pipeline(False)
    pipe.hset('login:', token, user)
    pipe.zadd('recent:', token, timestamp)
    if item:
        pipe.zadd('viewed:' + token, item, timestamp)
        pipe.zremrangebyrank('viewed:' + token, 0, -26)
        pipe.zincrby('viewed:', item, -1)
    pipe.execute()
