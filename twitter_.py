# coding=utf-8

import time
from lock_ import acquire_lock_with_timeout, release_lock

HOME_TIMELINE_SIZE = 1000
POST_PER_PASS = 1000

"""
user:uuid           hash

login               jason
id
name
followers
following
posts
singup

----------------

users:             hash
llogin             id
________________


user:id:           string
________________


status:2344646079877432   hash
message
posted
id
uid
login                     jason
________________


status:id:         string
________________


home:13487654        zset
2344646079877432     134576722

_________________

profile:13487654        zset
2344646079877432     134576722
_________________

followers:uuid          zset
uuid                    time.time()
_________________

following:uuid          zset
uuid                    time.time()
_________________

status:id           string
__________________

"""


def create_user(conn, login, name):
    llogin = login.lower()
    lock = acquire_lock_with_timeout(conn, 'user:' + llogin, 1)
    if not lock:
        return None

    if conn.hget('users:', llogin):
        release_lock(conn, 'user:' + llogin, lock)
        return None

    id = conn.incr('user:id:')
    pipeline = conn.pipeline(True)
    pipeline.hset('users:', llogin, id)
    pipeline.hmset('user:{}'.format(id), {
        'login': login,
        'id': id,
        'name': name,
        'followers': 0,
        'following': 0,
        'posts': 0,
        'singup': time.time()
    })

    pipeline.execute()
    release_lock(conn, 'user:' + llogin, lock)
    return id


def create_status(conn, uid, message, **data):
    pipeline = conn.pipeline(True)
    pipeline.hget('user:{}'.format(uid), 'login')
    pipeline.incr('status:id:')
    login, id = pipeline.execute()
    if not login:
        return None

    data.update({
        'message': message,
        'posted': time.time(),
        'id': id,
        'uid': uid,
        'login': login
    })
    pipeline.hmset('status:{}'.format(id), data)
    pipeline.hincrby('user:{}'.format(uid), 'posts')
    pipeline.execute()
    return id


def get_status_message(conn, uid, timeline='home:', page=1, count=30):
    statuses = conn.zrevrange(timeline + uid, (page - 1) * count, page * count - 1)  # 从大到小排序
    pipeline = conn.pipeline(True)
    for id in statuses:
        pipeline.hgetall('status:' + id)

    return filter(None, pipeline.execute())


def delete_status(conn, uid, status_id):
    key = 'status:' + status_id
    lock = acquire_lock_with_timeout(conn, key, 1)
    if not lock:
        return None

    if conn.hget(key, 'uid') != str(uid):
        release_lock(conn, key, lock)
        return None

    pipeline = conn.pipeline(True)
    pipeline.delete(key)
    pipeline.zrem('profile:' + uid, status_id)
    pipeline.zrem('home:' + uid, status_id)
    pipeline.hincrby('user:' + uid, 'posts', -1)
    pipeline.execute()
    release_lock(conn, key, lock)
    return True


def follow_user(conn, uid, other_id):
    key1 = 'following:' + uid
    key2 = 'followers:' + other_id

    if conn.zscore(key1, other_id):
        return None

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zadd(key1, other_id, now)
    pipeline.zadd(key2, uid, now)

    pipeline.zrevrange('profile:' + other_id, 0, HOME_TIMELINE_SIZE - 1, withscores=True)
    following, followers, status_and_score = pipeline.execute()[-3:]

    pipeline.hincrby('user:' + uid, 'following', int(following))
    pipeline.hincrby('user:' + other_id, 'followers', int(followers))
    if status_and_score:
        pipeline.zadd('home:' + uid, **dict(status_and_score))
    pipeline.zremrangebyrank('home:' + uid, 0, -HOME_TIMELINE_SIZE - 1)
    pipeline.execute()
    return True


def unfollow_user(conn, uid, other_id):
    key1 = 'following:' + uid
    key2 = 'followers:' + other_id

    if not conn.zscore(key1, other_id):
        return None

    pipeline = conn.pipeline(True)
    pipeline.zrem(key1, other_id)
    pipeline.zrem(key2, uid)
    pipeline.zrevrange('profile:' + other_id, 0, HOME_TIMELINE_SIZE - 1)
    following, followers, statuses = pipeline.execute()[-3:]

    pipeline.hincrby('user:' + uid, 'following', int(following))
    pipeline.hincrby('user:' + other_id, 'followers', int(followers))
    if statuses:
        pipeline.zrem('home:' + uid, *statuses)

    pipeline.execute()
    return True


def post_status(conn, uid, message, **data):
    id = create_status(conn, uid, message, **data)
    if not id:
        return None

    posted = conn.hget('status:' + id, 'posted')  # 获取消息发布时间
    if not posted:
        return None

    post = {str(id): float(posted)}
    conn.zadd('profile:' + uid, **post)  # 将状态消息添加到个人信息

    # 将状态消息推送给用户的关注者
    syndicate_status(conn, uid, post)
    return id


def syndicate_status(conn, uid, post, start=0):
    # 获取一千个关注者
    followers = conn.zrangebyscore('followers:' + uid, start, 'inf', start=0, num=POST_PER_PASS,
                                   withscores=True)

    pipeline = conn.pipeline(False)
    for follower, start in followers:
        pipeline.zadd('home:' + follower, **post)
        pipeline.zremrangebyrank('home:' + follower, 0, -HOME_TIMELINE_SIZE-1)
    pipeline.execute()

    if len(followers) >= POST_PER_PASS:
        execute_later(conn, 'default', 'syndicate_status', [conn, uid, post, start])
