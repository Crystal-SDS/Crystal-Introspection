from threading import Thread
from datetime import datetime, timedelta
from eventlet import greenthread
import Queue
import socket
import time
import pytz
import pika
import redis
import json
import copy
import os


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

    def publish_stateful_metric(self, routing_key, data):
        self.publish_thread.publish_statefull(routing_key, data)

    def publish_stateless_metric(self, routing_key, data):
        self.publish_thread.publish_stateless(routing_key,  data)

    def force_publish_metric(self, routing_key, data):
        self.publish_thread.force_publish_metric(routing_key, data)


class PublishThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

        self.logger = logger
        self.monitoring_statefull_data = dict()
        self.monitoring_stateless_data = dict()
        self.messages_to_send = Queue.Queue()

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

    def publish_statefull(self, metric_name, data):
        if metric_name not in self.monitoring_statefull_data:
            self.monitoring_statefull_data[metric_name] = dict()

        value = data['value']
        del data['value']
        key = str(data)

        if key not in self.monitoring_statefull_data[metric_name]:
            self.monitoring_statefull_data[metric_name][key] = 0

        try:
            self.monitoring_statefull_data[metric_name][key] += value
        except Exception as e:
            print e

    def publish_stateless(self, metric_name, data):
        if metric_name not in self.monitoring_stateless_data:
            self.monitoring_stateless_data[metric_name] = dict()

        value = data['value']
        del data['value']
        key = str(data)

        if key not in self.monitoring_stateless_data[metric_name]:
            self.monitoring_stateless_data[metric_name][key] = 0

        try:
            self.monitoring_stateless_data[metric_name][key] += value
        except Exception as e:
            print e

    def force_publish_metric(self, metric_name, data):
        date = datetime.now(pytz.timezone(time.tzname[0]))

        data['host'] = self.host_name
        data['metric_name'] = metric_name
        data['@timestamp'] = str(date.isoformat())

        routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
        message = dict()
        message[routing_key] = data
        self.messages_to_send.put(message)

    def _generate_messages_from_stateless_data(self, date, last_date):
        stateless_data_copy = copy.deepcopy(self.monitoring_stateless_data)

        if last_date == date.strftime("%Y-%m-%d %H:%M:%S"):
            self.last_stateless_data = copy.deepcopy(stateless_data_copy)
            return

        for metric_name in stateless_data_copy.keys():
            for key in stateless_data_copy[metric_name].keys():
                # example: {"{'project': 'crystal', 'container': 'crystal/data_1', 'method': 'GET'}": 52,
                #           "{'project': 'crystal', 'container': 'crystal/data_2', 'method': 'PUT'}": 31}

                if metric_name not in self.zero_value_timeout:
                    self.zero_value_timeout[metric_name] = dict()
                if key not in self.zero_value_timeout[metric_name]:
                    self.zero_value_timeout[metric_name][key] = 0

                if self.last_stateless_data and \
                   metric_name in self.last_stateless_data and \
                   key in self.last_stateless_data[metric_name]:
                    value = stateless_data_copy[metric_name][key] - \
                            self.last_stateless_data[metric_name][key]
                else:
                    # send value = 0 for second-1 for pretty printing into Kibana
                    pre_data = eval(key)
                    pre_data['host'] = self.host_name
                    pre_data['metric_name'] = metric_name
                    d = date - timedelta(seconds=1)
                    pre_data['@timestamp'] = str(d.isoformat())
                    pre_data['value'] = 0

                    routing_key = 'metric.'+pre_data['method'].lower()+'_'+metric_name
                    message = dict()
                    message[routing_key] = pre_data
                    self.messages_to_send.put(message)

                    value = stateless_data_copy[metric_name][key]

                if value == 0.0:
                    self.zero_value_timeout[metric_name][key] += 1
                    if self.zero_value_timeout[metric_name][key] == 5:
                        del self.monitoring_stateless_data[metric_name][key]
                        if len(self.monitoring_stateless_data[metric_name]) == 0:
                            del self.monitoring_stateless_data[metric_name]
                        del self.last_stateless_data[metric_name][key]
                        if len(self.last_stateless_data[metric_name]) == 0:
                            del self.last_stateless_data[metric_name]
                        del self.zero_value_timeout[metric_name][key]
                        if len(self.zero_value_timeout[metric_name]) == 0:
                            del self.zero_value_timeout[metric_name]
                else:
                    self.zero_value_timeout[metric_name][key] = 0

                data = eval(key)
                data['host'] = self.host_name
                data['metric_name'] = metric_name
                data['@timestamp'] = str(date.isoformat())
                data['value'] = value

                routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
                message = dict()
                message[routing_key] = data
                self.messages_to_send.put(message)

        self.last_stateless_data = copy.deepcopy(stateless_data_copy)

    def _generate_messages_from_statefull_data(self, date, last_date):

        if last_date == date.strftime("%Y-%m-%d %H:%M:%S"):
            return

        statefull_data_copy = copy.deepcopy(self.monitoring_statefull_data)

        for metric_name in statefull_data_copy.keys():
            for key in statefull_data_copy[metric_name].keys():

                if metric_name not in self.zero_value_timeout:
                    self.zero_value_timeout[metric_name] = dict()
                if key not in self.zero_value_timeout[metric_name]:
                    self.zero_value_timeout[metric_name][key] = 0

                if self.last_statefull_data and \
                   metric_name in self.last_statefull_data and \
                   key in self.last_statefull_data[metric_name]:
                    value = statefull_data_copy[metric_name][key]
                else:
                    # send value = 0 for second-1 for pretty printing into Kibana
                    pre_data = eval(key)
                    pre_data['host'] = self.host_name
                    pre_data['metric_name'] = metric_name
                    d = date - timedelta(seconds=1)
                    pre_data['@timestamp'] = str(d.isoformat())
                    pre_data['value'] = 0

                    routing_key = 'metric.'+pre_data['method'].lower()+'_'+metric_name
                    message = dict()
                    message[routing_key] = pre_data
                    self.messages_to_send.put(message)

                    value = statefull_data_copy[metric_name][key]

                if value == 0:
                    self.zero_value_timeout[metric_name][key] += 1
                    if self.zero_value_timeout[metric_name][key] == 5:
                        del self.monitoring_statefull_data[metric_name][key]
                        if len(self.monitoring_statefull_data[metric_name]) == 0:
                            del self.monitoring_statefull_data[metric_name]
                        del self.last_statefull_data[metric_name][key]
                        if len(self.last_statefull_data[metric_name]) == 0:
                            del self.last_statefull_data[metric_name]
                        del self.zero_value_timeout[metric_name][key]
                        if len(self.zero_value_timeout[metric_name]) == 0:
                            del self.zero_value_timeout[metric_name]
                else:
                    self.zero_value_timeout[metric_name][key] = 0

                data = eval(key)
                data['host'] = self.host_name
                data['metric_name'] = metric_name
                data['@timestamp'] = str(date.isoformat())
                data['value'] = value

                routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
                message = dict()
                message[routing_key] = data
                self.messages_to_send.put(message)

        self.last_statefull_data = copy.deepcopy(statefull_data_copy)

    def run(self):
        last_date = None
        self.last_stateless_data = None
        self.last_statefull_data = None
        self.zero_value_timeout = dict()

        self.rabbit = pika.BlockingConnection(self.parameters)
        self.channel = self.rabbit.channel()

        while True:
            greenthread.sleep(self.interval)
            date = datetime.now(pytz.timezone(time.tzname[0]))
            self._generate_messages_from_stateless_data(date, last_date)
            self._generate_messages_from_statefull_data(date, last_date)
            last_date = date.strftime("%Y-%m-%d %H:%M:%S")

            try:
                while not self.messages_to_send.empty():
                    message = self.messages_to_send.get()
                    for routing_key in message:
                        data = message[routing_key]
                        self.channel.basic_publish(exchange=self.exchange,
                                                   routing_key=routing_key,
                                                   body=json.dumps(data))
            except:
                self.messages_to_send.put(message)
                self.rabbit = pika.BlockingConnection(self.parameters)
                self.channel = self.rabbit.channel()


class ControlThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

        self.conf = conf
        self.logger = logger
        self.server = self.conf.get('execution_server')
        self.interval = self.conf.get('control_interval', 10)
        self.redis_host = self.conf.get('redis_host')
        self.redis_port = self.conf.get('redis_port')
        self.redis_db = self.conf.get('redis_db')

        self.redis = redis.StrictRedis(self.redis_host,
                                       self.redis_port,
                                       self.redis_db)

        self.metric_list = {}

    def _get_workload_metrics(self):
        """
        This method connects to redis to download the metrics and the
        information introduced via the dashboard.
        """
        metric_keys = self.redis.keys("workload_metric:*")
        metric_list = dict()
        for key in metric_keys:
            metric = self.redis.hgetall(key)
            if metric['execution_server'] == self.server and \
               metric['enabled'] == 'True':
                metric_list[key] = metric

        return metric_list

    def run(self):
        while True:
            try:
                self.metric_list = self._get_workload_metrics()
            except:
                self.logger.error("Unable to connect to " + self.redis_host +
                                  " for getting the workload metrics.")
            greenthread.sleep(self.interval)


class NodeStatusThread(Thread):

    def __init__(self, conf, logger):
        Thread.__init__(self)

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

        self.metric_list = {}

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
