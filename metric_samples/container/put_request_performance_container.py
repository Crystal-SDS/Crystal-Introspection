from crystal_introspection_middleware.metrics.abstract_metric import AbstractMetric
import time


class PutRequestPerformanceContainer(AbstractMetric):
    
    def execute(self):
        self.type = 'force'
        
        if self.method == "PUT" and self._is_object_request():
            self._intercept_put()
            self.start_time = time.time()
            self.request_size = 0
            
        return self.request

    def on_read(self, chunk):
        self.request_size += len(chunk)

    def on_finish(self):       
        transfer_perf = ((self.request_size/(time.time()-self.start_time))/1024.0)/1024
        self.register_metric(self.account+"/"+self.container, transfer_perf)
