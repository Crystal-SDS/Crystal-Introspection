#!/usr/bin/python

'''
This script creates a rabbit communication and a queue to get all messages
sent by a metric to print them out on screen. This is for testing purpose on
the metric middelware for Swift as part of the Crystal project.

@author Daniel Barcelona
'''

import sys
import signal

import pika
import json

# Configure your RabbitMQ here:
RABBIT_CREDENTIALS = pika.PlainCredentials('guest', 'guest')
RABBIT_HOST = 'localhost'
RABBIT_PORT = 5672


def callback(ch, method, properties, body):
    data = json.loads(body)
    print data


def stop(signal, frame):
    print "Stop consuming."
    channel.stop_consuming()
    channel.connection.close()
    sys.exit(0)


if (len(sys.argv) == 2):
    parameters = pika.ConnectionParameters(host=RABBIT_HOST,
                                           port=RABBIT_PORT,
                                           credentials=RABBIT_CREDENTIALS)
    channel = pika.BlockingConnection(parameters).channel()

    metric_id = sys.argv[1]
    routing_key = 'metrics.' + metric_id

    channel.queue_declare(queue=metric_id)

    channel.queue_bind(exchange="amq.topic",
                       queue=metric_id,
                       routing_key=routing_key)
    consumer = channel.basic_consume(callback, queue=metric_id, no_ack=True)

    signal.signal(signal.SIGINT, stop)

    channel.start_consuming()

else:
    print 'Wrong format: python deploy_metric.py <metric_name>'
