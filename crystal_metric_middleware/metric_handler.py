from metrics.metric_control import CrystalMetricControl
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger
from eventlet import greenthread
import sys
import os


class NotCrystalMetricRequest(Exception):
    pass


def _request_instance_property():
    """
    Set and retrieve the request instance.
    This works to force to tie the consistency between the request path and
    self.vars (i.e. api_version, account, container, obj) even if unexpectedly
    (separately) assigned.
    """

    def getter(self):
        return self._request

    def setter(self, request):
        self._request = request
        try:
            self._extract_vaco()
        except ValueError:
            raise NotCrystalMetricRequest()

    return property(getter, setter,
                    doc="Force to tie the request to acc/con/obj vars")


class CrystalMetricHandler(object):

    request = _request_instance_property()

    def __init__(self, request, conf, app, logger, crystal_control):
        self.exec_server = conf.get('execution_server')
        self.app = app
        self.logger = logger
        self.conf = conf
        self.request = request
        self.method = self.request.method
        self.response = None
        self.crystal_control = crystal_control

        # Add source directories to sys path
        workload_metrics = os.path.join('/opt', 'crystal', 'workload_metrics')
        sys.path.insert(0, workload_metrics)

        self._start_control_threads()

    def _extract_vaco(self):
        """
        Set version, account, container, obj vars from self._parse_vaco result
        :raises ValueError: if self._parse_vaco raises ValueError while
                            parsing, this method doesn't care and raise it to
                            upper caller.
        """
        if self.exec_server == 'proxy':
            self._api_version, self._account, self._container, self._obj = \
                self.request.split_path(4, 4, rest_with_last=True)
        elif self.exec_server == 'object':
            _, _, self._account, self._container, self._obj = \
                self.request.split_path(5, 5, rest_with_last=True)
        else:
            raise NotCrystalMetricRequest()

    def _start_control_threads(self):
        if not self.crystal_control.threads_started:
            try:
                self.logger.info("Starting threads.")
                self.crystal_control.publish_thread.start()
                self.crystal_control.control_thread.start()
                self.crystal_control.threads_started = True
                greenthread.sleep(0.1)
            except:
                self.logger.error("Error starting threads.")

    def _import_metric(self, metric):
        modulename = metric['metric_name'].rsplit('.', 1)[0]
        classname = metric['class_name']
        m = __import__(modulename, globals(), locals(), [classname])
        m_class = getattr(m, classname)
        metric_class = m_class(self.logger, self.crystal_control, modulename,
                               self.exec_server, self.request, self.response)
        return metric_class

    def handle_request(self):
            metrics = self.crystal_control.get_metrics()

            if self.method == 'PUT':
                for metric_key in metrics:
                    metric = metrics[metric_key]
                    if metric['in_flow'] == 'True':
                        metric_class = self._import_metric(metric)
                        self.request = metric_class.execute()

                if hasattr(self.request.environ['wsgi.input'], 'metrics'):
                    metric_list = list()
                    for metric in self.request.environ['wsgi.input'].metrics:
                        metric_list.append(metric.metric_name)
                    self.logger.info('Go to execute metrics on PUT request: ' +
                                     str(metric_list))

                return self.request.get_response(self.app)

            elif self.method == 'GET':
                self.response = self.request.get_response(self.app)

                if self.response.is_success:
                    for metric_key in metrics:
                        metric = metrics[metric_key]
                        if metric['out_flow'] == 'True':
                            metric_class = self._import_metric(metric)
                            self.response = metric_class.execute()

                if hasattr(self.response.app_iter, 'metrics'):
                    metric_list = list()
                    for metric in self.response.app_iter.metrics:
                        metric_list.append(metric.metric_name)
                    self.logger.info('Go to execute metrics on GET request: ' +
                                     str(metric_list))

                return self.response
            else:
                return self.request.get_response(self.app)


class CrystalMetricMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.exec_server = conf.get('execution_server')
        self.logger = get_logger(conf, name=self.exec_server +
                                 "-server Crystal Metrics",
                                 log_route='crystal_metric_handler')
        self.conf = conf
        self.handler_class = CrystalMetricHandler
        self.control_class = CrystalMetricControl

        ''' Singleton instance of Metric control '''
        self.crystal_control = self.control_class(conf=self.conf,
                                                  log=self.logger)

    @wsgify
    def __call__(self, req):
        try:
            if self.exec_server == 'object':
                raise NotCrystalMetricRequest
            request_handler = self.handler_class(req, self.conf,
                                                 self.app, self.logger,
                                                 self.crystal_control)
            self.logger.debug('call in %s' % (self.exec_server))
        except NotCrystalMetricRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Middleware execution failed')
            raise
        except Exception:
            self.logger.exception('Middleware execution failed')
            raise HTTPInternalServerError(body='Crystal Metric middleware execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""

    conf = global_conf.copy()
    conf.update(local_conf)

    conf['execution_server'] = conf.get('execution_server', 'proxy')
    conf['region_id'] = conf.get('region_id', 1)
    conf['zone_id'] = conf.get('zone_id', 1)

    conf['rabbit_host'] = conf.get('rabbit_host', 'controller')
    conf['rabbit_port'] = int(conf.get('rabbit_port', 5672))
    conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    conf['rabbit_password'] = conf.get('rabbit_password', 'openstack')

    conf['redis_host'] = conf.get('redis_host', 'controller')
    conf['redis_port'] = int(conf.get('redis_port', 6379))
    conf['redis_db'] = int(conf.get('redis_db', 0))

    conf['devices'] = conf.get('devices', '/srv/node')

    def swift_crystal_metric_middleware(app):
        return CrystalMetricMiddleware(app, conf)

    return swift_crystal_metric_middleware
