from crystal_introspection_middleware.metrics.abstract_metric import AbstractMetric

class GetOpsContainer(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        if self.method == "GET" and self._is_object_request():
            self.register_metric(self.account+"/"+self.container,1)
                
        return self.response    
