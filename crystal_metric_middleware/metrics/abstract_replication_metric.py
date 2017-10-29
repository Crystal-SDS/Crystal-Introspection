from datetime import datetime
from eventlet import Timeout
import pytz
import time

CHUNK_SIZE = 64 * 1024


class AbstractReplicationMetric(object):

    type = 'stateless'

    def __init__(self, logger, stateless_metrics_queue, statefull_metrics_queue,
                 instant_metrics_queue, metric_name, server, request, response):
        self.logger = logger
        self.request = request
        self.response = response

        self.stateless_metrics = stateless_metrics_queue
        self.statefull_metrics = statefull_metrics_queue
        self.instant_metrics = instant_metrics_queue

        self.metric_name = metric_name
        self.current_server = server
        self.method = self.request.method
        self.read_timeout = 30  # seconds

        self.data = {}
        self.data['method'] = self.method
        self.data['server_type'] = self.current_server
        self.data['source_node'] = request.environ['REMOTE_ADDR']
        self.data['metric_name'] = self.metric_name

    def register_metric(self, value):
        """
        Send data to publish thread
        """
        if self.type == 'stateful':
            self.statefull_metrics.put((self.data, value))
        elif self.type == 'stateless':
            self.stateless_metrics.put((self.data, value))
        elif self.type == 'force':
            date = datetime.now(pytz.timezone(time.tzname[0]))
            self.instant_metrics.put((self.data, date, value))

    def _is_ssync_already_intercepted(self):
        return isinstance(self.request.environ['wsgi.input'], IterReplication)

    def _get_applied_metrics_on_ssync(self):
        if hasattr(self.request.environ['wsgi.input'], 'metrics'):
            metrics = self.request.environ['wsgi.input'].metrics
            self.request.environ['wsgi.input'].metrics = list()
            return metrics
        else:
            return list()

    def _get_reader(self):
        if not self._is_ssync_already_intercepted():
            reader = self.request.environ['wsgi.input']
        else:
            reader = self.request.environ['wsgi.input'].obj_data

        return reader

    def _intercept_ssync(self):
        reader = self._get_reader()
        metrics = self._get_applied_metrics_on_ssync()
        metrics.append(self)

        self.request.environ['wsgi.input'] = IterReplication(reader, metrics, self.read_timeout)

    def execute(self):
        self._intercept_ssync()
        self.on_start()
        return self.request

    def on_start(self):
        pass

    def on_read(self, chunk):
        pass

    def on_finish(self):
        pass


class IterReplication(object):

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
        try:
            self.obj_data.close()
        except AttributeError:
            pass
        self.closed = True

    def __del__(self):
        self.close()
