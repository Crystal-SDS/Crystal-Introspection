from metrics.get_metrics import GetMetricsThread
from node_status import NodeStatusThread
from datetime import datetime, timedelta
from swift.common.swob import wsgify
from swift.common.utils import get_logger
from eventlet import greenthread
import Queue as Queue
import threading
import socket
import time
import pytz
import pika
import json
import copy
import sys
import os


# Add source directories to sys path
workload_metrics_path = os.path.join('/opt', 'crystal', 'workload_metrics')
sys.path.insert(0, workload_metrics_path)


class NotCrystalMetricRequest(Exception):
    pass


class CrystalMetricMiddleware(object):

    node_status = None
    threadLock = threading.Lock()

    def __init__(self, app, conf):
        self.app = app
        self.exec_server = conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server +
                                 "-server Crystal Metrics",
                                 log_route='crystal_metric_handler')
        self.conf = conf

        # Initialize node status thread only once
        if CrystalMetricMiddleware.node_status is None:
            CrystalMetricMiddleware.threadLock.acquire()
            if CrystalMetricMiddleware.node_status is None:
                self._start_node_status_thread()
            CrystalMetricMiddleware.threadLock.release()

        self._start_publisher_thread()
        self._start_get_metrics_thread()

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
        self.metric_publisher = PublishThread(self.conf, self.logger)
        self.metric_publisher.daemon = True
        self.metric_publisher.start()

    def _start_get_metrics_thread(self):
        self.logger.info('Starting metrics getter thread')
        self.metrics_getter = GetMetricsThread(self.conf, self.logger)
        self.metrics_getter.daemon = True
        self.metrics_getter.start()

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

        sl_metrics_q = self.metric_publisher.stateless_metrics_q
        sf_metrics_q = self.metric_publisher.statefull_metrics_q
        it_metrics_q = self.metric_publisher.instant_metrics_q

        metric_class = m_class(self.logger, sl_metrics_q, sf_metrics_q,
                               it_metrics_q, modulename, self._account,
                               self.exec_server, self.request, self.response)
        return metric_class

    @wsgify
    def __call__(self, req):
        self.request = req
        if not self.is_crystal_metric_request:
            return self.request.get_response(self.app)

        self.logger.debug('%s call in %s-server.' % (self.request.method,
                                                     self.exec_server))
        try:
            metrics = self.metrics_getter.get_metrics()

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
            self.logger.debug('%s call in %s-server: Bypassing middleware' %
                              (req.method, self.exec_server))
            return self.request.get_response(self.app)


class PublishThread(threading.Thread):

    def __init__(self, conf, logger):
        super(PublishThread, self).__init__()

        self.logger = logger
        self.messages_to_send = Queue.Queue()

        self.stateless_metrics_q = Queue.Queue()
        self.statefull_metrics_q = Queue.Queue()
        self.instant_metrics_q = Queue.Queue()

        self.interval = conf.get('publish_interval', 0.990)
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

    def _generate_messages_from_stateless_metrics(self, date, last_date):

        stateless_metrics = {}

        while not self.stateless_metrics_q.empty():
            (metric_data, value) = self.stateless_metrics_q.get_nowait()

            metric_name = metric_data['metric_name']
            key = str(metric_data)

            if metric_name not in stateless_metrics:
                stateless_metrics[metric_name] = dict()

            if key not in stateless_metrics[metric_name]:
                stateless_metrics[metric_name][key] = 0

            stateless_metrics[metric_name][key] += value

        if last_date:
            last_datetime = last_date.strftime("%Y-%m-%d %H:%M:%S")
            now_datetime = date.strftime("%Y-%m-%d %H:%M:%S")

            if last_datetime == now_datetime:
                return

        if self.last_stateless_metrics:
            for metric_name in self.last_stateless_metrics.keys():
                for key in self.last_stateless_metrics[metric_name].keys():
                    if metric_name not in stateless_metrics:
                        stateless_metrics[metric_name] = {}
                    if key not in stateless_metrics[metric_name]:
                        stateless_metrics[metric_name][key] = 0
                        self.zero_value_timeout[metric_name][key] += 1

                        if self.zero_value_timeout[metric_name][key] == 10:
                            del stateless_metrics[metric_name][key]
                            if len(stateless_metrics[metric_name]) == 0:
                                del stateless_metrics[metric_name]
                            del self.last_stateless_metrics[metric_name][key]
                            if len(self.last_stateless_metrics[metric_name]) == 0:
                                del self.last_stateless_metrics[metric_name]
                            del self.zero_value_timeout[metric_name][key]
                            if len(self.zero_value_timeout[metric_name]) == 0:
                                del self.zero_value_timeout[metric_name]
                    else:
                        self.zero_value_timeout[metric_name][key] = 0

        for metric_name in stateless_metrics.keys():
            for key in stateless_metrics[metric_name].keys():
                if metric_name not in self.zero_value_timeout:
                    self.zero_value_timeout[metric_name] = dict()
                if key not in self.zero_value_timeout[metric_name]:
                    self.zero_value_timeout[metric_name][key] = 0

                if self.last_stateless_metrics and \
                   metric_name in self.last_stateless_metrics and \
                   key in self.last_stateless_metrics[metric_name]:
                    value = stateless_metrics[metric_name][key]
                else:
                    # send value = 0 for second-1
                    pre_data = eval(key)
                    pre_data['host'] = self.host_name
                    d = date - timedelta(seconds=1)
                    pre_data['@timestamp'] = str(d.isoformat())
                    pre_data['value'] = 0

                    routing_key = 'metric.'+pre_data['method'].lower()+'_'+metric_name
                    message = dict()
                    message[routing_key] = pre_data
                    self.messages_to_send.put(message)

                    value = stateless_metrics[metric_name][key]

                data = eval(key)
                data['host'] = self.host_name
                data['@timestamp'] = str(date.isoformat())
                data['value'] = value

                routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
                message = dict()
                message[routing_key] = data
                self.messages_to_send.put(message)

        self.last_stateless_metrics = stateless_metrics

    def _generate_messages_from_statefull_metrics(self, date, last_date):

        while not self.statefull_metrics_q.empty():
            (metric_data, value) = self.statefull_metrics_q.get_nowait()

            metric_name = metric_data['metric_name']
            key = str(metric_data)

            if metric_name not in self.statefull_metrics:
                self.statefull_metrics[metric_name] = dict()

            if key not in self.statefull_metrics[metric_name]:
                self.statefull_metrics[metric_name][key] = 0

            self.statefull_metrics[metric_name][key] += value

        if last_date:
            last_datetime = last_date.strftime("%Y-%m-%d %H:%M:%S")
            now_datetime = date.strftime("%Y-%m-%d %H:%M:%S")

            if last_datetime == now_datetime:
                return

        for metric_name in self.statefull_metrics.keys():
            for key in self.statefull_metrics[metric_name].keys():

                if metric_name not in self.zero_value_timeout:
                    self.zero_value_timeout[metric_name] = dict()
                if key not in self.zero_value_timeout[metric_name]:
                    self.zero_value_timeout[metric_name][key] = 0

                if self.last_statefull_metrics and \
                   metric_name in self.last_statefull_metrics and \
                   key in self.last_statefull_metrics[metric_name]:
                    value = self.statefull_metrics[metric_name][key]
                else:
                    # send value = 0 for second-1
                    pre_data = eval(key)
                    pre_data['host'] = self.host_name
                    d = date - timedelta(seconds=1)
                    pre_data['@timestamp'] = str(d.isoformat())
                    pre_data['value'] = 0

                    routing_key = 'metric.'+pre_data['method'].lower()+'_'+metric_name
                    message = dict()
                    message[routing_key] = pre_data
                    self.messages_to_send.put(message)

                    value = self.statefull_metrics[metric_name][key]

                if value == 0:
                    self.zero_value_timeout[metric_name][key] += 1
                    if self.zero_value_timeout[metric_name][key] == 10:
                        del self.statefull_metrics[metric_name][key]
                        if len(self.statefull_metrics[metric_name]) == 0:
                            del self.statefull_metrics[metric_name]
                        del self.last_statefull_metrics[metric_name][key]
                        if len(self.last_statefull_metrics[metric_name]) == 0:
                            del self.last_statefull_metrics[metric_name]
                        del self.zero_value_timeout[metric_name][key]
                        if len(self.zero_value_timeout[metric_name]) == 0:
                            del self.zero_value_timeout[metric_name]
                else:
                    self.zero_value_timeout[metric_name][key] = 0

                data = eval(key)
                data['host'] = self.host_name
                data['@timestamp'] = str(date.isoformat())
                data['value'] = value

                routing_key = 'metric.'+data['method'].lower()+'_'+metric_name
                message = dict()
                message[routing_key] = data
                self.messages_to_send.put(message)

        self.last_statefull_metrics = copy.deepcopy(self.statefull_metrics)

    def _generate_messages_from_instant_metrics(self):
        while not self.instant_metrics_q.empty():
            (metric_data, date, value) = self.instant_metrics_q.get_nowait()
            metric_data['host'] = self.host_name
            metric_data['value'] = value
            metric_data['@timestamp'] = str(date.isoformat())

            routing_key = 'metric.'+metric_data['method'].lower()+'_'+metric_data['metric_name']
            message = dict()
            message[routing_key] = metric_data
            self.messages_to_send.put(message)

    def run(self):
        last_date = None
        self.last_stateless_metrics = None
        self.statefull_metrics = dict()
        self.last_statefull_metrics = None
        self.zero_value_timeout = dict()

        self.rabbit = pika.BlockingConnection(self.parameters)
        self.channel = self.rabbit.channel()

        while True:
            greenthread.sleep(self.interval)
            date = datetime.now(pytz.timezone(time.tzname[0]))
            self._generate_messages_from_stateless_metrics(date, last_date)
            self._generate_messages_from_statefull_metrics(date, last_date)
            self._generate_messages_from_instant_metrics()
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
