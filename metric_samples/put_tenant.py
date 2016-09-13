from abstract_metric import AbstractMetric

class PutTenant(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        if self.method == "PUT" and self._is_object_request():
            self.register_metric(self.account,1)
                
        return self.request
