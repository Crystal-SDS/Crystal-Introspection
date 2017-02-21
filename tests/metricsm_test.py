"""
Crystal SDS
Unit test for the metrics middelware. This will deploy a GetOps metric on Swift
and generate various get calls in one second to test if messages are sent to a
RabbitMQ queue.
"""

import unittest
import uuid
import os
import errno
import socket
from shutil import copyfile
import time
from threading import Thread

import redis
import swiftclient
import pika
import json

# Configure Redis here:
REDIS_CON_POOL = redis.ConnectionPool(host='localhost', port=6379, db=0)
WORKLOAD_METRICS_DIR = os.path.join("/opt", "crystal", "workload_metrics")

# Configure RabbitMQ here:
RABBIT_CREDENTIALS = pika.PlainCredentials('guest', 'guest')
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672

# Swift AUTH v1
# auth_url_v1 = 'http://127.0.0.1:8080/auth/v1.0'
# user = 'test:tester'
# key = 'testing'

# swift_metric_account_name = 'Swift'
# swift_metric_account_id = 'AUTH_test'

# Swift AUTH v2
auth_url_v2 = 'http://localhost:5000/v2.0/'
v2user = 'admin'
v2key = 'admin'
tenant = 'crystaltest'

# Configure your project id here following the format:
swift_project_idauth = 'AUTH_366756dbfd024e0aa7f204a7498dfcfa'


class MetricTestCase(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)
        self.r = redis.Redis(connection_pool=REDIS_CON_POOL)
        self.swift = swiftclient.client.Connection(authurl=auth_url_v2,
                                                   user=v2user,
                                                   key=v2key,
                                                   tenant_name=tenant,
                                                   auth_version="2.0")
        # self.swift = swiftclient.client.Connection(authurl=auth_url_v1,
        #                                            user=user,
        #                                            key=key)
        self.container_name = 'testc-' + str(uuid.uuid4())
        self.swift.put_container(self.container_name, {})
        self.object_name = 'testo-' + str(uuid.uuid4())

        parameters = pika.ConnectionParameters(host=RABBIT_HOST,
                                               port=RABBIT_PORT,
                                               credentials=RABBIT_CREDENTIALS)
        self.channel = pika.BlockingConnection(parameters).channel()

        f = open(self.object_name, 'w')
        f.close()

        # Copy metric file
        metric_file = os.path.abspath('get_ops_test.py')
        self.metric_name = os.path.basename(metric_file)
        try:
            os.makedirs(WORKLOAD_METRICS_DIR)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

        copyfile(metric_file, WORKLOAD_METRICS_DIR + '/' + self.metric_name)
        os.chmod(WORKLOAD_METRICS_DIR + '/' + self.metric_name, 0777)

        # Load GetOps to REDIS
        data = {}
        self.workload_metric_id = self.r.incr("workload_metrics:id")
        data['id'] = self.workload_metric_id
        data['metric_name'] = self.metric_name
        data['class_name'] = 'GetOpsTest'
        data['enabled'] = 'True'
        data['in_flow'] = 'False'
        data['out_flow'] = 'True'
        data['execution_server'] = 'proxy'

        self.r.hmset('workload_metric:' + str(self.workload_metric_id), data)

        self.metric_id = 'get_ops_test'
        routing_key = 'metrics.' + self.metric_id

        self.channel.queue_declare(queue=self.metric_id)

        self.channel.queue_bind(exchange="amq.topic",
                                queue=self.metric_id,
                                routing_key=routing_key)

    def tearDown(self):
        self.r.delete('workload_metric:' + str(self.workload_metric_id))
        self.r.decr("workload_metrics:id")
        os.remove(self.object_name)
        os.remove(WORKLOAD_METRICS_DIR + '/' + self.metric_name)
        self.channel.queue_delete(queue=self.metric_id)
        self.channel.connection.close()
        self.swift.delete_container(self.container_name)
        self.swift.close()

    def test_load_metrics(self):
        with open(self.object_name, 'r') as object_file:
            self.swift.put_object(self.container_name, self.object_name,
                                  contents=object_file.read(),
                                  content_type='text/plain')

        self.received = False
        self.stop = False

        t = Thread(target=self.consumer, args=[self.basic_queue_consume])
        t.start()

        self.swift.get_object(self.container_name, self.object_name)
        self.swift.get_object(self.container_name, self.object_name)
        time.sleep(2)
        self.stop = True
        time.sleep(2)
        self.swift.delete_object(self.container_name, self.object_name)

        self.assertTrue(self.received)

    def test_get_metric(self):
        with open(self.object_name, 'r') as object_file:
            self.swift.put_object(self.container_name, self.object_name,
                                  contents=object_file.read(),
                                  content_type='text/plain')

        self.received = False
        self.count = 0
        self.stop = False
        self.host = socket.gethostname()
        self.account = swift_project_idauth
        # self.account = swift_metric_account_name + "#:#" + swift_metric_account_id

        t = Thread(target=self.consumer, args=[self.count_consume])
        t.start()

        self.swift.get_object(self.container_name, self.object_name)
        self.swift.get_object(self.container_name, self.object_name)
        time.sleep(1)
        self.stop = True
        time.sleep(1)
        self.swift.delete_object(self.container_name, self.object_name)

        self.assertTrue(self.received)
        self.assertEquals(self.count, 2)

    # Aux methods

    def consumer(self, callback):
        self.channel.basic_consume(callback,
                                   queue=self.metric_id, no_ack=True)
        self.timeout = self.channel.connection.add_timeout(.3, self.callback)
        self.channel.start_consuming()
        self.channel.connection.remove_timeout(self.timeout)

    def callback(self):
        if self.stop:
            self.channel.stop_consuming()
        else:
            self.timeout = self.channel.connection.add_timeout(.3, self.callback)

    def basic_queue_consume(self, ch, method, properties, body):
        print "one message!"
        print json.loads(body)
        self.received = True

    def count_consume(self, ch, method, properties, body):
        # if not self.received:
        self.received = True
        data = json.loads(body)
        if self.count < data[self.host][self.account]:
            self.count = data[self.host][self.account]
