import mysql.connector
import httplib
import hashlib
import time 
import datetime
from datetime import datetime
from datetime import timedelta
from pymongo import MongoClient


class MongoUrlManager:
    def __init__(self, SERVER_IP = 'localhost', port=27017, client=None):
        # if a client object is not passed 
        # then try connecting to mongodb at the default localhost port 
        self.client = MongoClient(SERVER_IP, port) if client is None else client
        #create collection to store cached webpages,
        # which is the equivalent of a table in a relational database
        self.db = self.client.spider

    def dequeueUrl(self):
        record = self.db.mfw.find_one_and_update(
            {'status': 'new'}, 
            { '$set': { 'status' : 'downloading'} }, 
            { 'upsert':False, 'returnNewDocument' : False} 
        )
        if record:
            return record
        else:
            return None

    def enqueueUrl(self, url, status, depth):
        try:
            self.db.mfw.insert({'_id': url, 'status': status, 'queue_time': datetime.utcnow(), 'depth': depth})
        except Exception, Arguments:
            pass

    def finishUrl(self, url, status='done'):
        record = {'status': status, 'done_time': datetime.utcnow()}
        self.db.mfw.update({'_id': url}, {'$set': record}, upsert=False)

    def clear(self):
        self.db.mfw.drop()