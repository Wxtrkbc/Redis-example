# -*- coding: utf-8 -*-

import time

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432
ARTICLES_PER_PAGE = 25

"""
 article:100408   hash

 title
 link
 votes
 time
 poster

 ***********

 time  zset

 article:100408   1332065475.47

***************

 score   zset

 article:100408    2000

****************

 voted:100408   set

 user:234487
 user:132097

"""


def article_vote(conn, user, article):
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    if conn.zscore('time', 'article') < cutoff:  # 获取name对应有序集合中 value 对应的分数
        return

    article_id = article.partition(':')[-1]
    if conn.sadd("voted:" + article_id, user):  # 返回1表示该用户不存在于该集合
        conn.zincrby('score', article, VOTE_SCORE)  # 自增score对应的有序集合的article对应的分数
        conn.hincrby(article, 'votes', 1)  # 自增article对应的hash中的指定votes的值，不存在则创建votes=amount


def post_article(conn, user, title, link):
    now = time.time()
    article_id = str(conn.incr('article'))  # 将 article 存储的数值加1
    article = 'article:' + article_id
    voted = 'voted:' + article_id
    conn.sadd(voted, user)  # 自己也算一票
    conn.expire(voted, ONE_WEEK_IN_SECONDS)

    conn.hmset(article, {
        'title': title,
        'link': link,
        'poster': user,
        'time': now,
        'votes': 1,
    })

    conn.zadd('score', article, now + VOTE_SCORE)
    conn.zadd('time', article, now)

    return article_id


def get_articles(conn, page, order='score'):
    start = (page - 1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    ids = conn.zrevrange(order, start, end)  # 从大到小排序， 获取文章id列表
    articles = []
    for id in ids:
        article_data = conn.hgetall(id)    # 获取文章id对应的所有键值，性能较慢
        article_data['id'] = id
        articles.append(article_data)
    return articles


def add_remove_groups(conn, article_id, to_add=(), to_remove=()):
    article = 'article:' + article_id
    for group in to_add:
        conn.sadd('group:' + group, article)
    for group in to_remove:
        conn.srem('group:' + group, article)  # 在对应的集合中删除某些值


def get_group_articles(conn, group, page, order='score'):
    key = order + group   # 为每个群组的每种排序创建一个键
    if not conn.exists(key):

        # 获取两个集合的交集，如果遇到相同值不同分数，则按照aggregate进行操作, 如果是集合的话默认分值就是1
        conn.zinterstore(key, ['group:' + group, order], aggregate='max')
        conn.expire(key, 60)
    return get_articles(conn, page, key)
