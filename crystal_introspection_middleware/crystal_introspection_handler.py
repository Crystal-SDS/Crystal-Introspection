from crystal_introspection_control import CrystalIntrospectionControl
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger
import time
import json

PACKAGE_NAME = __name__.split('.')[0]

class CrystalIntrospectionHandler():

    def __init__(self, request, conf, app, logger, crystal_control):
        self.exec_server = conf.get('execution_server')
        self.app = app
        self.logger = logger
        self.conf = conf
        self.request = request
        self.response = None
        self.crystal_control = crystal_control
        self._start_control_threads()
        
    def _start_control_threads(self):
        if not self.crystal_control.threads_started:
            try:
                self.logger.info("Crystal - Starting introspection threads.")
                self.crystal_control.publish_thread.start()
                self.crystal_control.control_thread.start()
                self.crystal_control.threads_started = True
                time.sleep(0.1)
            except:
                self.logger.info("Crystal - Error starting introspection threads.")
            
    def _import_metric(self,metric):
        (modulename, classname) = metric.rsplit('.', 1)
        m = __import__(PACKAGE_NAME+'.'+modulename, globals(), locals(), [classname])
        m_class = getattr(m, classname) 
        metric_class = m_class(self.logger, self.crystal_control, modulename, 
                               self.exec_server, self.request, self.response)
        return metric_class

    def handle_request(self):
        metrics = self.crystal_control.get_metrics()

        for metric in metrics:
            params = json.loads(metrics[metric].replace("'",'"').lower())
            if params['in_flow']:
                self.logger.info("Go to execute metric on request: "+metric)
                metric_class = self._import_metric(metric)            
                self.request = metric_class.execute()

        self.response = self.request.get_response(self.app)
        
        for metric in metrics:
            params = json.loads(metrics[metric].replace("'",'"').lower())
            if params['out_flow']:
                self.logger.info("Go to execute metric on response: "+metric)
                metric_class = self._import_metric(metric)            
                self.response = metric_class.execute()
        
        return self.response    

class CrystalIntrospectionHandlerMiddleware(object):

    def __init__(self, app, conf, crystal_conf):
        self.app = app
        self.logger = get_logger(conf, log_route='sds_storlet_handler')
        self.conf = crystal_conf
        self.handler_class = CrystalIntrospectionHandler
        self.control_class = CrystalIntrospectionControl
        
        ''' Singleton instance of Introspection control '''
        self.crystal_control =  self.control_class.Instance(conf = self.conf,
                                                            log = self.logger)
        
    @wsgify
    def __call__(self, req):
        try:
            request_handler = self.handler_class(req, self.conf,
                                                 self.app, self.logger,
                                                 self.crystal_control)
            self.logger.debug('crystal_introspection_handler call')
        except:
            return req.get_response(self.app)

        try:
            return request_handler.handle_request()
        except HTTPException:
            self.logger.exception('Crystal Introspection execution failed')
            raise
        except Exception:
            self.logger.exception('Crystal Introspection execution failed')
            raise HTTPInternalServerError(body='Crystal Introspection execution failed')


def filter_factory(global_conf, **local_conf):
    """Standard filter factory to use the middleware with paste.deploy"""
    
    conf = global_conf.copy()
    conf.update(local_conf)

    crystal_conf = dict()
    crystal_conf['execution_server'] = conf.get('execution_server', 'object')
    
    crystal_conf['rabbit_host'] = conf.get('rabbit_host', 'controller')
    crystal_conf['rabbit_port'] = conf.get('rabbit_port', 5672)
    crystal_conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    crystal_conf['rabbit_password'] = conf.get('rabbit_password', 
                                               'rabbitmqastl1a4b4')

    crystal_conf['redis_host'] = conf.get('redis_host', 'controller')
    crystal_conf['redis_port'] = conf.get('redis_port', 6379)
    crystal_conf['redis_db'] = conf.get('redis_db', 0)
    
    crystal_conf['bind_ip'] = conf.get('bind_ip')
    crystal_conf['bind_port'] = conf.get('bind_port')


    def swift_crystal_introspection_middleware(app):
        return CrystalIntrospectionHandlerMiddleware(app, conf, crystal_conf)

    return swift_crystal_introspection_middleware
