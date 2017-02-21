from redis_ import get_redis
import unittest
import vote
from pprint import pprint


class TestVote(unittest.TestCase):
    def setUp(self):
        self.conn = get_redis()
        vote.post_article(self.conn, 'username', 'A title', 'http://www.google.com')

    def tearDown(self):

        to_del = (
            self.conn.keys('time*') + self.conn.keys('voted:*') + self.conn.keys('score*') +
            self.conn.keys('article:*') + self.conn.keys('group:*')
        )
        if to_del:
            self.conn.delete(*to_del)
        del self.conn

    def test_get_article(self):

        rep = self.conn.hgetall('article:' + '1')
        self.assertEqual(rep['poster'], 'username')

    def test_vote(self):
        vote.article_vote(self.conn, 'other_user', 'article:' + '1')
        self.assertEqual(int(self.conn.hget('article:' + '1', 'votes'), 2))

        articles = vote.get_articles(self.conn, 1)
        pprint(articles)

        self.assertTrue(len(articles) >= 1)

    def test_add_group(self):
        vote.add_remove_groups(self.conn, 1, ('new-group', ))
        articles = vote.get_group_articles(self.conn, ('new-group', ), 1)
        pprint(articles)

        self.assertTrue(len(articles) >= 1)

