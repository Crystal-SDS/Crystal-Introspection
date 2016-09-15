from crystal_introspection_middleware.metrics.abstract_metric import AbstractMetric

class ActiveGetRequestsContainer(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        self.state = 'stateful'
        
        if self.method == "GET" and self._is_object_request():
            self._intercept_get()
            self.register_metric(self.account+"/"+self.container,1)

        return self.response
    
    def on_finish(self):
        self.register_metric(self.account+"/"+self.container,-1)
