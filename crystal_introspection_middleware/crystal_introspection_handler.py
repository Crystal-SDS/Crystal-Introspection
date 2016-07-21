from crystal_introspection_control import CrystalIntrospectionControl
from swift.common.swob import HTTPInternalServerError
from swift.common.swob import HTTPException
from swift.common.swob import wsgify
from swift.common.utils import get_logger
import time


PACKAGE_NAME = __name__.split('.')[0]


class CrystalIntrospectionHandler():

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
        
    def _start_control_threads(self):
        if not self.crystal_control.publish_thread_started:
            try:
                self.logger.info("Crystal Introspection - Starting threads.")
                self.crystal_control.publish_thread.start()
                self.crystal_control.control_thread.start()
                self.crystal_control.publish_thread_started = True
                time.sleep(0.1)
            except:
                self.logger.info("Crystal Introspection - Error starting threads.")
            
    def _import_metric(self,metric):
        modulename = 'metrics.'+metric['metric_name'].rsplit('.', 1)[0]
        classname = metric['class_name']        
        m = __import__(PACKAGE_NAME+'.'+modulename, globals(), locals(), [classname])
        m_class = getattr(m, classname) 
        metric_class = m_class(self.logger, self.crystal_control, modulename, 
                               self.exec_server, self.request, self.response)
        return metric_class
    
    def _is_valid_request(self):
        #return False
        return self.method == 'GET' or self.method == 'PUT'

    def handle_request(self):
        
        if self._is_valid_request():
            metrics = self.crystal_control.get_metrics()
            
            for metric_key in metrics:
                metric = metrics[metric_key]
                if metric['in_flow'] == 'True' and metric['enabled'] == 'True':
                    self.logger.info('Crystal Introspection - Go to execute '
                                     'metric on input flow: '+metric['metric_name'])
                    metric_class = self._import_metric(metric)            
                    self.request = metric_class.execute()
    
            self.response = self.request.get_response(self.app)
            
            # TODO(josep): check status 200 on response
            
            for metric_key in metrics:
                metric = metrics[metric_key]
                if metric['out_flow'] == 'True' and metric['enabled'] == 'True':
                    self.logger.info('Crystal Introspection - Go to execute '
                                     'metric on output flow: '+metric['metric_name'])
                    metric_class = self._import_metric(metric)            
                    self.response = metric_class.execute()
            
            return self.response
        
        return self.request.get_response(self.app)


class CrystalIntrospectionHandlerMiddleware(object):

    def __init__(self, app, conf, crystal_conf):
        self.app = app
        self.logger = get_logger(conf, log_route='crystal_introspection_handler')
        self.conf = crystal_conf
        self.handler_class = CrystalIntrospectionHandler
        self.control_class = CrystalIntrospectionControl
        
        ''' Singleton instance of Introspection control '''
        self.crystal_control =  self.control_class(conf = self.conf,
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
    crystal_conf['rabbit_port'] = int(conf.get('rabbit_port', 5672))
    crystal_conf['rabbit_username'] = conf.get('rabbit_username', 'openstack')
    crystal_conf['rabbit_password'] = conf.get('rabbit_password', 
                                               'rabbitmqastl1a4b4')

    crystal_conf['redis_host'] = conf.get('redis_host', 'controller')
    crystal_conf['redis_port'] = int(conf.get('redis_port', 6379))
    crystal_conf['redis_db'] = int(conf.get('redis_db', 0))
    
    crystal_conf['bind_ip'] = conf.get('bind_ip')
    crystal_conf['bind_port'] = conf.get('bind_port')
    crystal_conf['devices'] = conf.get('devices')


    def swift_crystal_introspection_middleware(app):
        return CrystalIntrospectionHandlerMiddleware(app, conf, crystal_conf)

    return swift_crystal_introspection_middleware
