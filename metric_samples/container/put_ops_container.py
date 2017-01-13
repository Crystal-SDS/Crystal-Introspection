from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric


class PutOpsContainer(AbstractMetric):

    def execute(self):
        """
        Execute Metric
        """
        if self.method == "PUT" and self._is_object_request():
            self.register_metric(self.account+"/"+self.container, 1)

        return self.request
