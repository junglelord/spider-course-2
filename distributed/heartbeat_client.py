import socket
import threading
import time
from socket_client import SocketClient
import protocol_constants as pc
import json

import argparse

class HeartBeatClient():

    server_status = pc.STATUS_RUNNING
    run_heartbeat = False
    client_id = -1
    hb_period = 5
    socket_client = SocketClient()

    def __init__(self):
        self.run_heartbeat = False

    def connect(self):
        register_request = {}
        register_request[pc.MSG_TYPE] = pc.REGISTER
        self.client_id = self.socket_client.send(json.dumps(register_request))
        if self.client_id is None:
            raise IOError('Connection Failed')

    def disconnect(self):
        register_request = {}
        register_request[pc.MSG_TYPE] = pc.UNREGISTER
        self.socket_client.send(json.dumps(register_request))

    def start(self):
        try:
            t = threading.Thread(target=self.heartbeat, name=None)
            # set daemon so main thread can exit when receives ctrl-c
            t.setDaemon(True)
            t.start()
        except Exception:
            print "Error: unable to start thread"

    def heartbeat(self):
        skip_wait = False
        while self.run_heartbeat:
            if skip_wait is False:
                time.sleep(self.hb_period)
            else:
                skip_wait = False
            try:
                hb_request = {}
                hb_request[pc.MSG_TYPE] = pc.HEARTBEAT
                hb_request[pc.CLIENT_ID] = self.client_id
                hb_response_data = self.socket_client.send(json.dumps(hb_request))

                # should be network error
                if hb_response_data is None:
                    continue
                
                # print 'Heart Beat response' + json.dumps(hb_response_data)
                response = json.loads(hb_response_data)

                err = response.get(pc.ERROR)
                if err is not None:
                    if err == pc.ERR_NOT_FOUND:
                        register_request = {}
                        register_request[pc.MSG_TYPE] = pc.REGISTER
                        self.client_id = self.socket_client.send(json.dumps(register_request))

                        # skip heartbeat period and send next heartbeat immediately
                        skip_wait = True
                        self.heartbeat()
                        return
                    return

                action = response.get(pc.ACTION_REQUIRED)
                if action is not None:
                    action_request = {}
                    if action == pc.PAUSE_REQUIRED:
                        self.server_status = pc.PAUSED
                        action_request[pc.MSG_TYPE] = pc.PAUSED
                    elif action == pc.PAUSE_REQUIRED:
                        self.server_status = pc.RESUMED
                        action_request[pc.MSG_TYPE] = pc.RESUMED
                    elif action == pc.SHUTDOWN_REQUIRED:
                        self.server_status = pc.SHUTDOWN
                        # stop heartbeat thread
                        return
                    action_request[pc.CLIENT_ID] = self.client_id
                    self.socket_client.send(json.dumps(action_request))
                else:
                    self.server_status = response[pc.SERVER_STATUS]

            except socket.error as msg:
                print 'Send Data Error. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
                self.server_status = pc.STATUS_CONNECTION_LOST

    def get_target_items(self, type):
        hb_request = {}
        hb_request[pc.MSG_TYPE] = type
        hb_request[pc.CLIENT_ID] = self.client_id
        response = self.socket_client.send(json.dumps(hb_request))
        return response[pc.DATA]

    def finish_target_items(self, type, items):
        hb_request = {}
        hb_request[pc.MSG_TYPE] = type
        hb_request[pc.CLIENT_ID] = self.client_id
        hb_request[pc.FINISHED_ITEMS] = json.dumps(items)
        self.socket_client.send(json.dumps(hb_request))