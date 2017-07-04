import hashlib

from socket_server import ServerSocket
import protocol_constants as pc
import json
import time
from thread import *

from mongo_mgr import MongoManager

import signal
import sys


constants = {
	'reorder_period': 1200, # 20 mins
	'connection_lost_period': 30, # 30s
}

class CrawlMaster:
	clients = {}

	server_status = pc.STATUS_RUNNING

	last_rereoder_time = time.time()

	mongo_mgr = MongoManager()

	def __init__(self, mongo_client = None, mongo_host='localhost'):
		self.server = ServerSocket(self.on_message)
		self.server.start()

	def on_message(self, msg):
		# print 'Heart Beat request' + msg
		request = json.loads(msg)
		type = request[pc.MSG_TYPE]
		client_state = {}
		response = {}
		response[pc.SERVER_STATUS] = self.server_status
		if type == pc.REGISTER:
			client_id = self.get_free_id()
			client_state['status'] = pc.STATUS_RUNNING
			client_state['time'] = time.time()
			self.clients[client_id] = client_state
			return client_id
		elif type == pc.UNREGISTER:
			client_id = request.get(pc.CLIENT_ID)
			del self.clients[client_id]
			return json.dumps(response)
		elif type == pc.LOCATIONS:
			items = self.mongo_mgr.dequeueItems(request[pc.REQUEST_SIZE])
			response[pc.MSG_TYPE] = pc.LOCATIONS
			response[pc.CRAWL_DELAY] = 2
			response[pc.DATA] = json.dumps(items)
			return json.dumps(response)
		elif type == pc.TRIPLES:
			items = self.mongo_mgr.dequeueItems(request[pc.REQUEST_SIZE])
			response[pc.MSG_TYPE] = pc.LOCATIONS
			response[pc.DATA] = json.dumps(items)
			return json.dumps(response)
		
		client_id = request.get(pc.CLIENT_ID)
		if client_id is None:
			response[pc.ERROR] = pc.ERR_NOT_FOUND
			return json.dumps(response)
		if type == pc.HEARTBEAT:
			if self.server_status is not self.clients[client_id]['status']:
				if self.server_status == pc.STATUS_RUNNING:
					response[pc.ACTION_REQUIRED] = pc.RESUME_REQUIRED
				elif self.server_status == pc.STATUS_PAUSED:
					response[pc.ACTION_REQUIRED] = pc.PAUSE_REQUIRED
				elif self.server_status == pc.STATUS_SHUTDOWN:
					response[pc.ACTION_REQUIRED] = pc.SHUTDOWN_REQUIRED
				return json.dumps(response)
		else:
			client_state['status'] = type
			client_state['time'] = time.time()
			self.clients[client_id] = client_state

		return json.dumps(response)

	def get_free_id(self):
		i = 0
		for key in self.clients:
			if i < int(key):
				break
			i += 1
		return str(i)

	def periodical_check(self):
		clients_status_ok = True

		for cid, state in self.clients.iteritems():
			# no heart beat for 2 mins, remove it
			if time.time() - state['time'] > constants['connection_lost_period']:
				# remove it from client list 
				# del client[cid]
				# set client status to be CONNECTION_LIST
				self.clients[cid]['status'] = pc.STATUS_CONNECTION_LOST
				continue

			if state['status'] != self.server_status:
				clients_status_ok = False
				break


def exit_signal_handler(signal, frame):
	crawl_master.server.close()
	sys.exit(1)

crawl_master = CrawlMaster()

start_new_thread(crawl_master.periodical_check, ())

signal.signal(signal.SIGINT, exit_signal_handler)
signal.pause()