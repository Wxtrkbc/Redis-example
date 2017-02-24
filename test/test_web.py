import json
import threading
import time
import unittest
import uuid

from redis_ import get_redis
from virtual_web import update_token, check_token, clean_session
from virtual_web import QUIT, LIMIT


class TestWeb(unittest.TestCase):
    def setUp(self):
        self.conn = get_redis()

    def tearDown(self):
        conn = self.conn
        to_del = (
            conn.keys('login:*') + conn.keys('recent:*') + conn.keys('viewed:*') +
            conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
            conn.keys('schedule:*') + conn.keys('inv:*'))
        if to_del:
            self.conn.delete(*to_del)
        del self.conn

    def test_login_cookies(self):
        conn = self.conn
        token = str(uuid.uuid4())
        update_token(conn, token, 'jason', 'apple')

        res = check_token(conn, token)
        self.assertEqual(str(res, encoding='utf-8'), 'jason')

        # t = threading.Thread(target=clean_session, args=(conn,))
        # t.setDaemon(1)
        # t.start()
        #
        # global QUIT
        # QUIT = True
        # time.sleep(2)
        # if t.isAlive():
        #     raise Exception("The clean sessions thread is still alive?!?")

        # s = conn.hlen('login:')
        # print(s)
        # self.assertFalse(s)

