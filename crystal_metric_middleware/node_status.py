from eventlet import greenthread
import threading
import redis
import time
import json
import os
import socket


class NodeStatusThread(threading.Thread):

    def __init__(self, conf, logger):
        super(NodeStatusThread, self).__init__()

        self.conf = conf
        self.logger = logger
        self.server = self.conf.get('execution_server')
        self.region_id = self.conf.get('region_id')
        self.zone_id = self.conf.get('zone_id')
        self.interval = self.conf.get('status_interval', 10)
        self.redis_host = self.conf.get('redis_host')
        self.redis_port = self.conf.get('redis_port')
        self.redis_db = self.conf.get('redis_db')

        self.host_name = socket.gethostname()
        self.host_ip = socket.gethostbyname(self.host_name)
        self.devices = self.conf.get('devices')

        self.redis = redis.StrictRedis(self.redis_host,
                                       self.redis_port,
                                       self.redis_db)

    def _get_swift_disk_usage(self):
        swift_devices = dict()
        if self.server == 'object':
            if self.devices and os.path.exists(self.devices):
                for disk in os.listdir(self.devices):
                    if disk.startswith('sd'):
                        statvfs = os.statvfs(self.devices+'/'+disk)
                        swift_devices[disk] = dict()
                        swift_devices[disk]['size'] = statvfs.f_frsize * statvfs.f_blocks
                        swift_devices[disk]['free'] = statvfs.f_frsize * statvfs.f_bfree

        return swift_devices

    def run(self):
        while True:
            try:
                swift_usage = self._get_swift_disk_usage()
                self.redis.hmset(self.server+'_node:'+self.host_name,
                                 {'type': self.server,
                                  'name': self.host_name,
                                  'ip': self.host_ip,
                                  'region_id': self.region_id,
                                  'zone_id': self.zone_id,
                                  'last_ping': time.time(),
                                  'devices': json.dumps(swift_usage)})
            except:
                self.logger.error("Unable to connect to " + self.redis_host +
                                  " for publishing the node status.")
            greenthread.sleep(self.interval)
