# coding=utf-8

import time
import uuid
from lock_ import acquire_lock, release_lock


def acquire_semaphore(conn, semname, limit, timeout=10):
    """
    semaphore:remote     zset
    uuid4             1326452023.421



    不公平信号量 导致原因各个系统的时间并不完全相同
    """
    identifier = str(uuid.uuid4())
    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zadd(semname, identifier, now)
    pipeline.zrank(semname, identifier)  # 返回有序集中指定成员的排名。分数值递增(从小到大)顺序排列
    if pipeline.execute()[-1] < limit:
        return identifier

    conn.zrem(semname, identifier)
    return None


def release_semaphore(conn, semname, identifier):
    """
    :return: True 正确释放，False表示该信号因为过期而被删除
    """
    return conn.zrem(semname, identifier)


def acquire_fair_semaphore(conn, semname, limit, timeout=10):
    """

    semaphore:remote            zset
    uuid4                       1326452023.421
    ------------

    semaphore:remote:owner       zset
    uuid4                        7361
    uuid4                        7365
    --------------

    semaphore:remote:counter     string
                                 7361
    """

    identifier = str(uuid.uuid4())
    czset = semname + ':owner'
    ctr = semname + ':counter'
    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zinrterstore(czset, {czset: 1, semname: 0})  # 取交集， 并且设置不同的权重

    pipeline.incr(ctr)
    counter = pipeline.execute()[-1]  # 对计算器自增，并获取自增后的值

    pipeline.zadd(semname, identifier, now)
    pipeline.zadd(czset, identifier, counter)
    pipeline.zrank(czset, identifier)  # 获取某个值在 name对应的有序集合中的排行（从 0 开始）
    if pipeline.execute()[-1] < limit:
        return identifier

    pipeline.zrem(semname, identifier)
    pipeline.zrem(czset, identifier)
    pipeline.execute()
    return None


def release_fair_semaphore(conn, semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname + ':owner', identifier)
    return pipeline.execute()[0]


def refresh_fair_semaphore(conn, semname, identifiler):
    if conn.zadd(semname, identifiler, time.time()):
        release_fair_semaphore(conn, semname, identifiler)
        return False
    return True


def acquire_semaphore_with_lock(conn, semname, limit, timeout=10):
    identifier = acquire_lock(conn, semname, acquire_timeout=.01)
    if identifier:
        try:
            return acquire_fair_semaphore(conn, semname, limit, timeout)
        finally:
            release_lock(conn, semname, identifier)