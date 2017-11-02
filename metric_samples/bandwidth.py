from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class Bandwidth(AbstractMetric):

    type = 'stateless'

    def on_read(self, chunk):
        mbytes = (len(chunk)/1024.0)/1024
        self.register_metric(mbytes)
