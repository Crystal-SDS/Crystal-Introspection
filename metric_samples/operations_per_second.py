from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class OperationsPerSecond(AbstractMetric):

    type = 'stateless'

    def on_start(self):
        self.register_metric(1)
