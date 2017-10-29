from crystal_metric_middleware.metrics.abstract_replication_metric import AbstractReplicationMetric
import eventlet


class ReplicationBandwidth(AbstractReplicationMetric):

    type = 'stateless'

    def on_start(self):
        self.mbytes = 0
        self.sender = eventlet.spawn(self.send_metric)

    def on_read(self, chunk):
        self.mbytes += (len(chunk)/1024.0)/1024

    def send_metric(self):
        while True:
            eventlet.sleep(0.1)
            if self.mbytes == 0:
                break
            self.register_metric(self.mbytes)
            self.mbytes = 0
