from abstract_metric import AbstractMetric

class PutContainer(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """
        if self.request.method == "PUT" and self._is_object_request():                
            self.register_metric(self.account+"/"+self.container,1)
        
        return self.request
