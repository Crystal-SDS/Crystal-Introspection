from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric
import time


class GetRequestPerformance(AbstractMetric):

    def execute(self):
        self.type = 'force'

        if self.method == "GET" and self._is_object_request():
            self._intercept_get()
            self.start_time = time.time()
            self.request_size = 0

        return self.response

    def on_read(self, chunk):
        self.request_size += len(chunk)

    def on_finish(self):
        transfer_perf = ((self.request_size/(time.time()-self.start_time))/1024.0)/1024
        self.register_metric(self.account, transfer_perf)
