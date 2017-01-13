from eventlet import Timeout
import select
import os

CHUNK_SIZE = 64 * 1024


class AbstractMetric(object):
    def __init__(self, logger, crystal_control, metric_name, server,
                 request, response):
        self.logger = logger
        self.request = request
        self.response = response
        self.crystal_control = crystal_control
        self.metric_name = metric_name
        self.current_server = server
        self.method = self.request.method
        self.type = 'stateless'
        self.read_timeout = 30  # seconds

        self._parse_vaco()
        self.account_name = self.request.headers['X-Project-Name']
        self.account = self.account_name + "#:#" + self.account_id

    def register_metric(self, key, value):
        """
        Send data to publish thread
        """
        routing_key = self.metric_name
        if self.type == 'stateful':
            self.crystal_control.publish_stateful_metric(routing_key,
                                                         key, value)
        elif self.type == 'stateless':
            self.crystal_control.publish_stateless_metric(routing_key,
                                                          key, value)
        elif self.type == 'force':
            self.crystal_control.force_publish_metric(routing_key,
                                                      key, value)

    def _is_object_request(self):
        if self.current_server == 'proxy':
            path = self.request.environ['PATH_INFO']
            if path.endswith('/'):
                path = path[:-1]
            splitted_path = path.split('/')
            if len(splitted_path) > 4:
                return True
        else:
            # TODO: Check for object-server
            return True

    def _is_get_already_intercepted(self):
        return isinstance(self.response.app_iter, IterLikeFileDescriptor) or \
               isinstance(self.response.app_iter, IterLikeGetProxy)

    def _is_put_already_intercepted(self):
        return isinstance(self.request.environ['wsgi.input'], IterLikePut)

    def _get_applied_metrics_on_get(self):
        if hasattr(self.response.app_iter, 'metrics'):
            metrics = self.response.app_iter.metrics
            self.response.app_iter.metrics = list()
            return metrics
        else:
            return list()

    def _get_applied_metrics_on_put(self):
        if hasattr(self.request.environ['wsgi.input'], 'metrics'):
            metrics = self.request.environ['wsgi.input'].metrics
            self.request.environ['wsgi.input'].metrics = list()
            return metrics
        else:
            return list()

    def _get_object_reader(self):

        if self.method == 'GET':
            if self._is_get_already_intercepted():
                reader = self.response.app_iter.obj_data
                self.response.app_iter.closed = True

            elif self.current_server == 'proxy':
                reader = self.response.app_iter

            elif self.current_server == 'object':
                reader = self.response.app_iter._fp

        elif self.method == "PUT" and not self._is_put_already_intercepted():
            reader = self.request.environ['wsgi.input']
        elif self.method == "PUT":
            reader = self.request.environ['wsgi.input'].obj_data

        return reader

    def _intercept_get(self):
        reader = self._get_object_reader()
        metrics = self._get_applied_metrics_on_get()
        metrics.append(self)

        if self.method == 'GET':
            if self.current_server == 'object':
                self.response.app_iter = IterLikeFileDescriptor(reader, metrics, self.read_timeout)
            if self.current_server == 'proxy':
                self.response.app_iter = IterLikeGetProxy(reader, metrics, self.read_timeout)

    def _intercept_put(self):
        reader = self._get_object_reader()
        metrics = self._get_applied_metrics_on_put()
        metrics.append(self)

        if self.method == 'PUT':
            self.request.environ['wsgi.input'] = IterLikePut(reader, metrics, self.read_timeout)

    def _parse_vaco(self):
        if self._is_object_request():
            if self.current_server == 'proxy':
                _, self.account_id, self.container, self.object = self.request.split_path(4, 4, rest_with_last=True)
            else:
                _, _, self.account_id, self.container, self.object = self.request.split_path(5, 5, rest_with_last=True)

    def execute(self):
        """ Execute Metric """
        raise NotImplementedError()

    def on_read(self, chunk):
        pass

    def on_finish(self):
        pass


class IterLike(object):

    def __init__(self, obj_data, metrics, timeout):
        self.closed = False
        self.obj_data = obj_data
        self.timeout = timeout
        self.metrics = metrics
        self.buf = b''

    def __iter__(self):
        return self

    def _apply_metrics_on_read(self, chunk):
        for metric in self.metrics:
            metric.on_read(chunk)

    def _apply_metrics_on_finish(self):
        for metric in self.metrics:
            metric.on_finish()

    def read_with_timeout(self, size):
        raise NotImplementedError()

    def next(self, size=CHUNK_SIZE):
        raise NotImplementedError()

    def _close_check(self):
        if self.closed:
            raise ValueError('I/O operation on closed file')

    def read(self, size=CHUNK_SIZE):
        self._close_check()
        return self.next(size)

    def readline(self, size=-1):
        self._close_check()

        # read data into self.buf if there is not enough data
        while b'\n' not in self.buf and \
              (size < 0 or len(self.buf) < size):
            if size < 0:
                chunk = self.read()
            else:
                chunk = self.read(size - len(self.buf))
            if not chunk:
                break
            self.buf += chunk

        # Retrieve one line from buf
        data, sep, rest = self.buf.partition(b'\n')
        data += sep
        self.buf = rest

        # cut out size from retrieved line
        if size >= 0 and len(data) > size:
            self.buf = data[size:] + self.buf
            data = data[:size]

        return data

    def readlines(self, sizehint=-1):
        self._close_check()
        lines = []
        try:
            while True:
                line = self.readline(sizehint)
                if not line:
                    break
                lines.append(line)
                if sizehint >= 0:
                    sizehint -= len(line)
                    if sizehint <= 0:
                        break
        except StopIteration:
            pass
        return lines

    def close(self):
        if self.closed:
            return
        self._apply_metrics_on_finish()
        self.obj_data.close()
        self.closed = True

    def __del__(self):
        self.close()


class IterLikePut(IterLike):

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = self.obj_data.read(size)
                self._apply_metrics_on_read(chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise

        return chunk

    def next(self, size=CHUNK_SIZE):
        if len(self.buf) < size:
            self.buf += self.read_with_timeout(size - len(self.buf))
            if self.buf == b'':
                self.close()
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def close(self):
        if self.closed:
            return
        self._apply_metrics_on_finish()
        self.closed = True


class IterLikeGetProxy(IterLike):

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = self.obj_data.next()
                self._apply_metrics_on_read(chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise
        return chunk

    def next(self, size=CHUNK_SIZE):
        if len(self.buf) < size:
            self.buf += self.read_with_timeout(size - len(self.buf))
            if self.buf == b'':
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data


class IterLikeFileDescriptor(IterLike):

    def read_with_timeout(self, size):
        try:
            with Timeout(self.timeout):
                chunk = os.read(self.obj_data, size)
                self._apply_metrics_on_read(chunk)
        except Timeout:
            self.close()
            raise
        except Exception:
            self.close()
            raise
        return chunk

    def next(self, size=64 * 1024):
        if len(self.buf) < size:
            r, _, _ = select.select([self.obj_data], [], [], self.timeout)
            if len(r) == 0:
                self.close()

            if self.obj_data in r:
                self.buf += self.read_with_timeout(size - len(self.buf))
                if self.buf == b'':
                    self.close()
                    raise StopIteration('Stopped iterator ex')
            else:
                raise StopIteration('Stopped iterator ex')

        if len(self.buf) > size:
            data = self.buf[:size]
            self.buf = self.buf[size:]
        else:
            data = self.buf
            self.buf = b''
        return data

    def close(self):
        if self.closed:
            return
        os.close(self.obj_data)
        self.closed = True
