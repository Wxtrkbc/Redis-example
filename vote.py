# -*- coding: utf-8 -*-

from redis_ import get_redis
import time

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432


"""
 article:100408   hash

 title
 link
 votes
 time

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
    if conn.sadd("voted:" + article_id, user):  # 返回0表示该用户存在于该集合
        conn.zincrby('score', article, VOTE_SCORE)  # 自增score对应的有序集合的article对应的分数
        conn.hincrby(article, 'votes', 1)    # 自增article对应的hash中的指定votes的值，不存在则创建votes=amount
