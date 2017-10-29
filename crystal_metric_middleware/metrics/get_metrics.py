import threading
import redis
from eventlet import greenthread


class GetMetricsThread(threading.Thread):

    def __init__(self, conf, logger):
        super(GetMetricsThread, self).__init__()

        self.conf = conf
        self.logger = logger
        self.server = self.conf.get('execution_server')
        self.interval = self.conf.get('control_interval', 1)
        self.redis_host = self.conf.get('redis_host')
        self.redis_port = self.conf.get('redis_port')
        self.redis_db = self.conf.get('redis_db')

        self.redis = redis.StrictRedis(self.redis_host,
                                       self.redis_port,
                                       self.redis_db)
        self.metrics = dict()

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
               metric['status'] == 'Running':
                metric_list[key] = metric

        return metric_list

    def run(self):
        while True:
            try:
                self.metrics = self._get_workload_metrics()
            except:
                self.logger.error("Unable to connect to " + self.redis_host +
                                  " for getting the workload metrics.")
            greenthread.sleep(self.interval)

    def get_metrics(self):
        return self.metrics
