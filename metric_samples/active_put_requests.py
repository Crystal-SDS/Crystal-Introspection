from abstract_metric import AbstractMetric

class ActivePutRequests(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        self.state = 'stateful'
        
        if self.method == "PUT" and self._is_object_request():
            self._intercept_put()
            self.register_metric(self.account,1)

        return self.request

    def on_finish(self):
        self.register_metric(self.account,-1)
