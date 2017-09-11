from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class ActiveRequests(AbstractMetric):

    type = 'stateful'

    def on_start(self):
        self.register_metric(+1)

    def on_finish(self):
        self.register_metric(-1)
