from crystal_metric_control import CrystalMetricControl
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger
import time


PACKAGE_NAME = __name__.split('.')[0]


class NotCrystalRequest(Exception):
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
            raise NotCrystalRequest()

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
            raise NotCrystalRequest()

    def _start_control_threads(self):
        if not self.crystal_control.threads_started:
            try:
                self.logger.info("Crystal Metric - Starting threads.")
                self.crystal_control.publish_thread.start()
                self.crystal_control.control_thread.start()
                self.crystal_control.threads_started = True
                time.sleep(0.1)
            except:
                self.logger.info("Crystal Metric - Error starting threads.")

    def _import_metric(self, metric):
        modulename = 'metrics.'+metric['metric_name'].rsplit('.', 1)[0]
        classname = metric['class_name']
        m = __import__(PACKAGE_NAME+'.'+modulename, globals(), locals(), [classname])
        m_class = getattr(m, classname)
        metric_class = m_class(self.logger, self.crystal_control, modulename,
                               self.exec_server, self.request, self.response)
        return metric_class

    @property
    def _is_valid_request(self):
        return self.method == 'GET' or self.method == 'PUT'

    def handle_request(self):

        if self._is_valid_request:
            metrics = self.crystal_control.get_metrics()

            for metric_key in metrics:
                metric = metrics[metric_key]
                if metric['in_flow'] == 'True':
                    metric_class = self._import_metric(metric)
                    self.request = metric_class.execute()

            if hasattr(self.request.environ['wsgi.input'], 'metrics'):
                metric_list = list()
                for metric in self.request.environ['wsgi.input'].metrics:
                    metric_list.append(metric.metric_name.split('.')[1])
                self.logger.info('Crystal Metric - Go to execute '
                                 'metrics on input flow: ' + str(metric_list))

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
                    metric_list.append(metric.metric_name.split('.')[1])
                self.logger.info('Crystal Introspection - Go to execute '
                                 'metrics on output flow: ' + str(metric_list))

            return self.response

        return self.request.get_response(self.app)


class CrystalMetricHandlerMiddleware(object):

    def __init__(self, app, conf, crystal_conf):
        self.app = app
        self.exec_server = conf.get('execution_server')
        self.logger = get_logger(conf, log_route='crystal_metric_handler')
        self.conf = crystal_conf
        self.handler_class = CrystalMetricHandler
        self.control_class = CrystalMetricControl

        ''' Singleton instance of Metric control '''
        self.crystal_control = self.control_class(conf=self.conf,
                                                  log=self.logger)

    @wsgify
    def __call__(self, req):
        try:
            if self.exec_server == 'object':
                raise NotCrystalRequest
            request_handler = self.handler_class(req, self.conf,
                                                 self.app, self.logger,
                                                 self.crystal_control)
            self.logger.debug('crystal_metric_handler call')
        except NotCrystalRequest:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Crystal Metric execution failed')
            raise
        except Exception:
            self.logger.exception('Crystal Metric execution failed')
            raise HTTPInternalServerError(body='Crystal Metric execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""

    conf = global_conf.copy()
    conf.update(local_conf)

    crystal_conf = dict()
    crystal_conf['execution_server'] = conf.get('execution_server', 'object')

    crystal_conf['rabbit_host'] = conf.get('rabbit_host', 'controller')
    crystal_conf['rabbit_port'] = int(conf.get('rabbit_port', 5672))
    crystal_conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    crystal_conf['rabbit_password'] = conf.get('rabbit_password', 'rabbitmqastl1a4b4')
    # crystal_conf['rabbit_username'] = conf.get('rabbit_username', 'test')
    # crystal_conf['rabbit_password'] = conf.get('rabbit_password', 'test')

    crystal_conf['redis_host'] = conf.get('redis_host', 'controller')
    crystal_conf['redis_port'] = int(conf.get('redis_port', 6379))
    crystal_conf['redis_db'] = int(conf.get('redis_db', 0))

    crystal_conf['bind_ip'] = conf.get('bind_ip')
    crystal_conf['bind_port'] = conf.get('bind_port')
    crystal_conf['devices'] = conf.get('devices')

    def swift_crystal_metric_middleware(app):
        return CrystalMetricHandlerMiddleware(app, conf, crystal_conf)

    return swift_crystal_metric_middleware
