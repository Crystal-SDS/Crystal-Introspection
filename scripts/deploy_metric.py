#!/usr/bin/python

"""
Script to install a metric on the metric middleware for Swift as part of the
Crystal project. This is for testing the metric middleware alone.

@author Daniel Barcelona
"""

import sys
import os.path
import errno
import importlib
from shutil import copyfile

import redis
from redis.exceptions import RedisError

# Configure your redis here:
REDIS_CON_POOL = redis.ConnectionPool(host='localhost', port=6379, db=0)
WORKLOAD_METRICS_DIR = os.path.join("/opt", "crystal", "workload_metrics")

if len(sys.argv) == 2:
    # Check file
    metric_file = os.path.abspath(sys.argv[1])

    if not os.path.isfile(metric_file):
        sys.exit("ERROR: The given file is missing.")

    metric_name = os.path.basename(metric_file)
    print "Adding metric from file", metric_file

    # Copy file
    try:
        os.makedirs(WORKLOAD_METRICS_DIR)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    copyfile(metric_file, WORKLOAD_METRICS_DIR + '/' + metric_name)
    os.chmod(WORKLOAD_METRICS_DIR + '/' + metric_name, 0777)

    print "File copied to", WORKLOAD_METRICS_DIR + '/' + metric_name

    # Check Redis connection
    try:
        r = redis.Redis(connection_pool=REDIS_CON_POOL)
    except RedisError:
        sys.exit('Error connecting with DB. \
                 Check parameter REDIS_CON_POOL of this script.')

    # Metric class name
    sys.path.append(WORKLOAD_METRICS_DIR)
    module = importlib.import_module(metric_name.split('.')[0])
    class_ = True
    while class_:
        metric_class = raw_input("Enter the metric class name: ")
        try:
            getattr(module, metric_class)
            class_ = False
        except AttributeError:
            print "Wrong class name.", metric_class, \
                "not defined in", metric_name

    # Inflow or outflow
    inflow = raw_input("Is it inflow? (y or n)")
    while inflow not in ['y', 'n']:
        inflow = raw_input("Please write 'y' or 'n'.")
    if inflow == "y":
        isin = 'True'
    else:
        isin = 'False'
    outflow = raw_input("Is it outflow? (y or n)")
    while outflow not in ['y', 'n']:
        outflow = raw_input("Please write 'y' or 'n'.")
    if outflow == "y":
        isout = 'True'
    else:
        isout = 'False'

    # Server where to execute
    server = raw_input("To execute on proxy or on object server?")
    while server not in ['proxy', 'object']:
        server = raw_input("Please write 'proxy' or 'object'.")

    # Load it to REDIS
    data = {}
    workload_metric_id = r.incr("workload_metrics:id")
    data['id'] = workload_metric_id
    data['metric_name'] = metric_name
    data['class_name'] = metric_class
    data['enabled'] = 'True'
    data['in_flow'] = isin
    data['out_flow'] = isout
    data['execution_server'] = server

    r.hmset('workload_metric:' + str(workload_metric_id), data)

else:
    print 'Wrong format: python deploy_metric.py <metric_file>.py'
