from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric
import eventlet


class Bandwidth(AbstractMetric):

    type = 'stateless'

    def on_start(self):
        self.mbytes = 0
        self.sender = eventlet.spawn(self.send_metric)

    def on_read(self, chunk):
        self.mbytes += (len(chunk)/1024.0)/1024

    def on_finish(self):
        eventlet.kill(self.sender)
        if self.mbytes != 0:
            self.register_metric(self.mbytes)

    def send_metric(self):
        while True:
            eventlet.sleep(1)
            self.register_metric(self.mbytes)
            self.mbytes = 0
