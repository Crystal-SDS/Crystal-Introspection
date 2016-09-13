'''
Created on Sep 13, 2016

@author: Raul
'''
from crystal_introspection_middleware.metrics.abstract_metric import AbstractMetric
import time


class PutRequestPerformance(AbstractMetric):
    
    def __init__(self, logger, crystal_control, metric_name, server, request, response):
        AbstractMetric.__init__(self, logger, crystal_control, metric_name, server, request, response)
        '''Store the current requests times and sizes'''
        self.start_time = 0
        self.request_size = 0
    
    def execute(self):
        
        if self.method == "PUT" and self._is_object_request():
            self._intercept_put()
            self.start_time = time.time()
            
        return self.request

    def on_read(self, chunk):
        self.request_size += len(chunk)

    def on_finish(self):        
        transfer_perf = self.request_size/(time.time()-self.start_time)
        self.register_metric(self.account, transfer_perf)
        