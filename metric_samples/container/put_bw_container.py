from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class PutBwContainer(AbstractMetric):
        
    def execute(self):
        """
        Execute Metric
        """ 
        if self.method == "PUT" and self._is_object_request():
            ''' When we intercept the request, all chunks will enter by run_metric method ''' 
            self._intercept_put()
            
        ''' It is necessary to return the intercepted request '''             
        return self.request
 
    def on_read(self, chunk):
        ''' In this case, the metric count the number of bytes '''
        mbytes = (len(chunk)/1024.0)/1024
        self.register_metric(self.account+"/"+self.container,mbytes)
