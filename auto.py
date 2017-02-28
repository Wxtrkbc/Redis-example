# coding=utf-8


def add_update_contact(conn, user, contact):
    ac_list = 'recent:' + user
    pipeline = conn.pipeline(True)
    pipeline.lrem(ac_list, contact)
    pipeline.lpush(ac_list, contact)
    pipeline.ltrim(ac_list, 0, 99)
    pipeline.execute()


def remove_contact(conn, user, contact):
    conn.lrem('recent:' + user, contact)


def fetch_auto_complete_list(conn, user, prefix):
    candidates = conn.lrange('recent:' + user, 0, -1)
    return [candidate for candidate in candidates if candidate.startwith(prefix)]
