# -*- coding: utf-8 -*-

import hashlib
from datetime import datetime
from datetime import timedelta

import redis

class RedisTermsManager:

    def get_category_list_name(self):
        return 'wiki_category'

    def get_article_list_name(self):
        return 'wiki_article'

    def __init__(self, sever_ip='localhost', client=None, expires=timedelta(days=30)):
        self.redis_client = redis.StrictRedis(host=sever_ip, port=6379, db=0)

    # Insert a new item into a collection specified by queue_name
    def enqueue_item(self, queue_name, item):
        num = self.redis_client.get(queue_name + item)
        if num is not None:
            self.redis_client.set(queue_name + item, int(num) + 1 )
            return num
        self.redis_client.set(queue_name + item, 1)
        self.redis_client.lpush(queue_name, item)
        return 0

    # Pop an item from a collection specified by queue_name
    def dequeue_item(self, queue_name):
        return self.redis_client.rpop(queue_name)

    Anaconda2
    def clear(self):
        self.redis_client.flushall()