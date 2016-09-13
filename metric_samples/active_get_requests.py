from abstract_metric import AbstractMetric

class ActiveGetRequests(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        self.state = 'stateful'
        
        if self.method == "GET" and self._is_object_request():
            self._intercept_get()
            self.register_metric(self.account,1)

        return self.response
    
    def on_finish(self):
        self.register_metric(self.account,-1)
