from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric
import time


class RequestPerformance(AbstractMetric):

    type = 'force'

    def on_start(self):
        self.start_time = time.time()
        self.request_size = 0

    def on_read(self, chunk):
        self.request_size += len(chunk)

    def on_finish(self):
        transfer_perf = ((self.request_size/(time.time()-self.start_time))/1024.0)/1024
        self.register_metric(transfer_perf)
