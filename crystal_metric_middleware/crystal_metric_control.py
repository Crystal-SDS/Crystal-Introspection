from threading import Thread
from datetime import datetime
import socket
import time
import pytz
import pika
import redis
import json
import os
from eventlet import greenthread

SRC_METRIC_PATH = os.path.join("/opt", "crystal", "workload_metrics")
DST_METRIC_PATH = os.path.abspath(__file__).rsplit('/', 1)[0]+'/metrics'


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):  # @NoSelf
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CrystalMetricControl(object):
    __metaclass__ = Singleton

    def __init__(self, conf, log):
        self.logger = log
        self.conf = conf

        self.status_thread = NodeStatusThread(self.conf, self.logger)
        self.status_thread.daemon = True
        self.status_thread.start()

        self.control_thread = ControlThread(self.conf, self.logger)
        self.control_thread.daemon = True

        self.publish_thread = PublishThread(self.conf, self.logger)
        self.publish_thread.daemon = True

        self.threads_started = False

    def get_metrics(self):
        return self.control_thread.metric_list

    def publish_stateful_metric(self, routing_key, key, value):
        self.publish_thread.publish_statefull(routing_key, key, value)

    def publish_stateless_metric(self, routing_key, key, value):
        self.publish_thread.publish_stateless(routing_key, key, value)

    def force_publish_metric(self, routing_key, key, value):
        self.publish_thread.force_publish_metric(routing_key, key, value)


class PublishThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

        self.logger = logger
        self.monitoring_statefull_data = dict()
        self.monitoring_stateless_data = dict()

        self.interval = conf.get('publish_interval', 0.995)
        # self.ip = conf.get('bind_ip')+":"+conf.get('bind_port')
        self.host_name = socket.gethostname()
        self.exchange = conf.get('exchange', 'amq.topic')

        rabbit_host = conf.get('rabbit_host')
        rabbit_port = int(conf.get('rabbit_port'))
        rabbit_user = conf.get('rabbit_username')
        rabbit_pass = conf.get('rabbit_password')

        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
        self.parameters = pika.ConnectionParameters(host=rabbit_host,
                                                    port=rabbit_port,
                                                    credentials=credentials)

    def publish_statefull(self, routing_key, key, value):
        if routing_key not in self.monitoring_statefull_data:
            self.monitoring_statefull_data[routing_key] = dict()

        if key not in self.monitoring_statefull_data[routing_key]:
            self.monitoring_statefull_data[routing_key][key] = 0

        try:
            self.monitoring_statefull_data[routing_key][key] += value
        except:
            pass

    def publish_stateless(self, routing_key, key, value):
        if routing_key not in self.monitoring_stateless_data:
            self.monitoring_stateless_data[routing_key] = dict()

        if key not in self.monitoring_stateless_data[routing_key]:
            self.monitoring_stateless_data[routing_key][key] = 0

        try:
            self.monitoring_stateless_data[routing_key][key] += value
        except:
            pass

    def force_publish_metric(self, routing_key, key, value):
        date = datetime.now(pytz.timezone(time.tzname[0]))
        rabbit = pika.BlockingConnection(self.parameters)
        channel = rabbit.channel()

        data = dict()
        data[self.host_name] = dict()
        data[self.host_name][key] = value
        data[self.host_name]['@timestamp'] = str(date.isoformat())

        channel.basic_publish(exchange=self.exchange,
                              routing_key=routing_key,
                              body=json.dumps(data))
        rabbit.close()

    def run(self):
        data = dict()
        rabbit = pika.BlockingConnection(self.parameters)
        channel = rabbit.channel()
        while True:
            greenthread.sleep(self.interval)
            date = datetime.now(pytz.timezone(time.tzname[0]))

            for routing_key in self.monitoring_stateless_data.keys():
                data[self.host_name] = self.monitoring_stateless_data[routing_key].copy()
                data[self.host_name]['@timestamp'] = str(date.isoformat())
                for key in self.monitoring_stateless_data[routing_key].keys():
                    if self.monitoring_stateless_data[routing_key][key] == 0:
                        del self.monitoring_stateless_data[routing_key][key]
                    else:
                        self.monitoring_stateless_data[routing_key][key] = 0

                if not self.monitoring_stateless_data[routing_key]:
                    del self.monitoring_stateless_data[routing_key]

                channel.basic_publish(exchange=self.exchange,
                                      routing_key=routing_key,
                                      body=json.dumps(data))

            for routing_key in self.monitoring_statefull_data.keys():
                data[self.host_name] = self.monitoring_statefull_data[routing_key].copy()
                data[self.host_name]['@timestamp'] = str(date.isoformat())
                for key in self.monitoring_statefull_data[routing_key].keys():
                    if self.monitoring_statefull_data[routing_key][key] == 0:
                        del self.monitoring_statefull_data[routing_key][key]

                if not self.monitoring_statefull_data[routing_key]:
                    del self.monitoring_statefull_data[routing_key]

                channel.basic_publish(exchange=self.exchange,
                                      routing_key=routing_key,
                                      body=json.dumps(data))


class ControlThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

        self.conf = conf
        self.logger = logger
        self.server = self.conf.get('execution_server')
        self.interval = self.conf.get('control_interval', 10)
        redis_host = self.conf.get('redis_host')
        redis_port = self.conf.get('redis_port')
        redis_db = self.conf.get('redis_db')

        self.redis = redis.StrictRedis(redis_host,
                                       redis_port,
                                       redis_db)

        self.metric_list = {}

    def _get_workload_metrics(self):
        """
        This method connects to redis to download new metrics the information
        introduced via the dashboard.
        """
        metric_keys = self.redis.keys("workload_metric:*")
        metric_list = dict()
        for key in metric_keys:
            metric = self.redis.hgetall(key)
            if metric['execution_server'] == self.server and \
               metric['enabled'] == 'True':
                metric_list[key] = metric
                file_name = metric_list[key]['metric_name']
                try:
                    src = os.path.join(SRC_METRIC_PATH, file_name)
                    lnk = os.path.join(DST_METRIC_PATH, file_name)
                    os.symlink(src, lnk)
                except OSError:
                    pass

        return metric_list

    def run(self):
        while True:
            self.metric_list = self._get_workload_metrics()
            greenthread.sleep(self.interval)


class NodeStatusThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

        self.conf = conf
        self.logger = logger
        self.server = self.conf.get('execution_server')
        self.interval = self.conf.get('status_interval', 10)
        redis_host = self.conf.get('redis_host')
        redis_port = self.conf.get('redis_port')
        redis_db = self.conf.get('redis_db')

        self.host_name = socket.gethostname()
        self.host_ip = socket.gethostbyname(self.host_name)
        self.devices = self.conf.get('devices')

        self.redis = redis.StrictRedis(redis_host,
                                       redis_port,
                                       redis_db)

        self.metric_list = {}

    def _get_swift_disk_usage(self):
        swift_devices = dict()
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
            swift_usage = self._get_swift_disk_usage()
            self.redis.hmset('node:'+self.host_name, {'type': self.server,
                                                      'name': self.host_name,
                                                      'ip': self.host_ip,
                                                      'last_ping': time.time(),
                                                      'devices': json.dumps(swift_usage)})
            greenthread.sleep(self.interval)
