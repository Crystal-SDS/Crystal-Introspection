from node_status import NodeStatusThread
from swift.common.swob import wsgify
from swift.common.utils import get_logger
from eventlet import greenthread
import threading
import sys
import os
from datetime import datetime, timedelta
from threading import Thread
import socket
import time
import pytz
import pika
import redis
import json
import copy
import Queue as pQueue
from multiprocessing import Queue


# Add source directories to sys path
workload_metrics_path = os.path.join('/opt', 'crystal', 'workload_metrics')
sys.path.insert(0, workload_metrics_path)


class NotCrystalMetricRequest(Exception):
    pass


class CrystalMetricMiddleware(object):

    stateless_metrics = None
    statefull_metrics = None
    instant_metrics = None
    metrics = None
    threadLock = threading.Lock()

    def __init__(self, app, conf):
        self.app = app
        self.exec_server = conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server +
                                 "-server Crystal Metrics",
                                 log_route='crystal_metric_handler')
        self.conf = conf

        # Initialize queues and start threads only once
        if CrystalMetricMiddleware.stateless_metrics is None \
           and CrystalMetricMiddleware.statefull_metrics is None:
            CrystalMetricMiddleware.threadLock.acquire()
            if CrystalMetricMiddleware.stateless_metrics is None \
               and CrystalMetricMiddleware.statefull_metrics is None \
               and CrystalMetricMiddleware.instant_metrics is None:
                CrystalMetricMiddleware.stateless_metrics = Queue()
                CrystalMetricMiddleware.statefull_metrics = Queue()
                CrystalMetricMiddleware.instant_metrics = Queue()
                CrystalMetricMiddleware.metrics = Queue()

                self._start_publisher_thread()
                self._start_get_metrics_thread()
                self._start_node_status_thread()
            CrystalMetricMiddleware.threadLock.release()

    @property
    def is_crystal_metric_request(self):
        try:
            valid = True
            self._extract_vaco()
        except:
            valid = False

        return self.request.method in ('PUT', 'GET') and valid

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        if self.exec_server == 'proxy':
            self._api_version, self._account, self._container, self._obj = \
                self.request.split_path(4, 4, rest_with_last=True)
        elif self.exec_server == 'object':
            _, _, self._account, self._container, self._obj = \
                self.request.split_path(5, 5, rest_with_last=True)
        else:
            raise NotCrystalMetricRequest()

    def _start_publisher_thread(self):
        self.logger.info('Starting publisher thread')
        CrystalMetricMiddleware.metric_publisher = PublishThread(self.conf, self.logger)
        CrystalMetricMiddleware.metric_publisher.daemon = True
        CrystalMetricMiddleware.metric_publisher.start()

    def _start_get_metrics_thread(self):
        self.logger.info('Starting metrics getter thread')
        CrystalMetricMiddleware.metrics_getter = GetMetricsThread(self.conf, self.logger)
        CrystalMetricMiddleware.metrics_getter.daemon = True
        CrystalMetricMiddleware.metrics_getter.start()

    def _start_node_status_thread(self):
        self.logger.info('Starting node status thread')
        CrystalMetricMiddleware.node_status = NodeStatusThread(self.conf, self.logger)
        CrystalMetricMiddleware.node_status.daemon = True
        CrystalMetricMiddleware.node_status.start()

    def _import_metric(self, metric):
        modulename = metric['metric_name'].rsplit('.', 1)[0]
        classname = metric['class_name']
        m = __import__(modulename, globals(), locals(), [classname])
        m_class = getattr(m, classname)

        sl_metrics_q = CrystalMetricMiddleware.stateless_metrics
        sf_metrics_q = CrystalMetricMiddleware.statefull_metrics
        it_metrics_q = CrystalMetricMiddleware.instant_metrics

        metric_class = m_class(self.logger, sl_metrics_q, sf_metrics_q,
                               it_metrics_q, modulename, self._account,
                               self.exec_server, self.request, self.response)
        return metric_class

    @wsgify
    def __call__(self, req):
        self.request = req
        if not self.is_crystal_metric_request:
            return self.request.get_response(self.app)

        self.logger.debug('%s call in %s-server.' % (self.request.method, self.exec_server))
        try:
            metrics = CrystalMetricMiddleware.metrics.get_nowait()
            print metrics

            if metrics and self.request.method == 'PUT':
                for metric_key in metrics:
                    metric = metrics[metric_key]
                    if metric['in_flow'] == 'True':
                        metric_class = self._import_metric(metric)
                        self.request = metric_class.execute()

                if hasattr(self.request.environ['wsgi.input'], 'metrics'):
                    metric_list = list()
                    for metric in self.request.environ['wsgi.input'].metrics:
                        metric_list.append(metric.metric_name)
                    self.logger.info('Go to execute metrics on PUT request: ' +
                                     str(metric_list))

                return self.request.get_response(self.app)

            elif metrics and self.request.method == 'GET':
                self.response = self.request.get_response(self.app)

                if self.response.is_success:
                    for metric_key in metrics:
                        metric = metrics[metric_key]
                        if metric['out_flow'] == 'True':
                            metric_class = self._import_metric(metric)
                            self.response = metric_class.execute()

                if hasattr(self.response.app_iter, 'metrics'):
                    metric_list = list()
                    for metric in self.response.app_iter.metrics:
                        metric_list.append(metric.metric_name)
                    self.logger.info('Go to execute metrics on GET request: ' +
                                     str(metric_list))

                return self.response
            else:
                self.logger.info('No metrics to execute')
                return req.get_response(self.app)
        except:
            self.logger.debug('%s call in %s-server: Bypassing middleware' % (req.method, self.exec_server))
            return self.request.get_response(self.app)


class PublishThread(threading.Thread):

    def __init__(self, conf, logger):
        super(PublishThread, self).__init__()

        self.logger = logger
        self.monitoring_statefull_data = dict()
        self.monitoring_stateless_data = dict()
        self.messages_to_send = pQueue.Queue()

        self.interval = conf.get('publish_interval', 0.995)
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

        self.statefull_agregator = Thread(target=self._aggregate_statefull_metrics)
        self.statefull_agregator.setDaemon(True)
        self.statefull_agregator.start()

        self.stateless_agregator = Thread(target=self._aggregate_stateless_metrics)
        self.stateless_agregator.setDaemon(True)
        self.stateless_agregator.start()

        self.instant_sender = Thread(target=self._send_instant_metrics)
        self.instant_sender.setDaemon(True)
        self.instant_sender.start()

    def _aggregate_statefull_metrics(self):
        while True:
            try:
                if not CrystalMetricMiddleware.statefull_metrics.empty():
                    (metric_name, data) = CrystalMetricMiddleware.statefull_metrics.get_nowait()
                    if metric_name not in self.monitoring_statefull_data:
                        self.monitoring_statefull_data[metric_name] = dict()

                    value = data['value']
                    del data['value']
                    key = str(data)

                    if key not in self.monitoring_statefull_data[metric_name]:
                        self.monitoring_statefull_data[metric_name][key] = 0

                    self.monitoring_statefull_data[metric_name][key] += value
                else:
                    greenthread.sleep(0.5)
            except Exception as e:
                print e.message

    def _aggregate_stateless_metrics(self):
        while True:
            try:
                if not CrystalMetricMiddleware.stateless_metrics.empty():
                    (metric_name, data) = CrystalMetricMiddleware.stateless_metrics.get_nowait()
                    print (metric_name, data)
                    if metric_name not in self.monitoring_stateless_data:
                        self.monitoring_stateless_data[metric_name] = dict()

                    value = data['value']
                    del data['value']
                    key = str(data)

                    if key not in self.monitoring_stateless_data[metric_name]:
                        self.monitoring_stateless_data[metric_name][key] = 0

                    self.monitoring_stateless_data[metric_name][key] += value
                else:
                    greenthread.sleep(0.5)
            except Exception as e:
                greenthread.sleep(self.interval)
                print e.message

    def _send_instant_metrics(self):
        while True:
            try:
                if not CrystalMetricMiddleware.instant_metrics.empty():
                    (metric_name, date, data) = CrystalMetricMiddleware.instant_metrics.get_nowait()
                    data['host'] = self.host_name
                    data['metric_name'] = metric_name
                    data['@timestamp'] = str(date.isoformat())

                    routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
                    message = dict()
                    message[routing_key] = data
                    self.messages_to_send.put(message)
                else:
                    greenthread.sleep(self.interval)
            except Exception as e:
                print e.message

    def _generate_messages_from_stateless_data(self, date, last_date):
        stateless_data_copy = copy.deepcopy(self.monitoring_stateless_data)

        if last_date:
            last_datetime = last_date.strftime("%Y-%m-%d %H:%M:%S")
            now_datetime = date.strftime("%Y-%m-%d %H:%M:%S")

            if last_datetime == now_datetime:
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
                    if self.zero_value_timeout[metric_name][key] == 10:
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

        if last_date:
            last_datetime = last_date.strftime("%Y-%m-%d %H:%M:%S")
            now_datetime = date.strftime("%Y-%m-%d %H:%M:%S")

            if last_datetime == now_datetime:
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
                    if self.zero_value_timeout[metric_name][key] == 10:
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
            last_date = date

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


class GetMetricsThread(threading.Thread):

    def __init__(self, conf, logger):
        super(GetMetricsThread, self).__init__()

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

    def _get_workload_metrics(self):
        """
        This method connects to redis to download the metrics and the
        information introduced via the dashboard.
        """
        metric_keys = self.redis.keys("workload_metric:*")
        metric_list = {}
        for key in metric_keys:
            metric = self.redis.hgetall(key)
            if self.server in metric['execution_server'] and \
               metric['enabled'] == 'True':
                metric_list[key] = metric

        return metric_list

    def run(self):
        while True:
            try:
                metrics = self._get_workload_metrics()
                CrystalMetricMiddleware.metrics.put_nowait(metrics)
            except:
                self.logger.error("Unable to connect to " + self.redis_host +
                                  " for getting the workload metrics.")
            greenthread.sleep(self.interval)


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""

    conf = global_conf.copy()
    conf.update(local_conf)

    conf['execution_server'] = conf.get('execution_server', 'proxy')
    conf['region_id'] = conf.get('region_id', 1)
    conf['zone_id'] = conf.get('zone_id', 1)

    conf['rabbit_host'] = conf.get('rabbit_host', 'controller')
    conf['rabbit_port'] = int(conf.get('rabbit_port', 5672))
    conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    conf['rabbit_password'] = conf.get('rabbit_password', 'openstack')

    conf['redis_host'] = conf.get('redis_host', 'controller')
    conf['redis_port'] = int(conf.get('redis_port', 6379))
    conf['redis_db'] = int(conf.get('redis_db', 0))

    conf['devices'] = conf.get('devices', '/srv/node')

    def swift_crystal_metric_middleware(app):
        return CrystalMetricMiddleware(app, conf)

    return swift_crystal_metric_middleware
