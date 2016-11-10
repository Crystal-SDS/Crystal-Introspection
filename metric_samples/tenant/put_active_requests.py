from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class PutActiveRequests(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        self.type = 'stateful'
        
        if self.method == "PUT" and self._is_object_request():
            self._intercept_put()
            self.register_metric(self.account,1)

        return self.request

    def on_finish(self):
        self.register_metric(self.account,-1)
