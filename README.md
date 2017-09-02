# Crystal Metric Middleware for OpenStack Swift

_Please visit [Crystal Installation](https://github.com/Crystal-SDS/INSTALLATION/) for an overview of all Crystal components._

Crystal Metric Middleware is a middleware for OpenStack Swift that dynamically manages monitoring metrics. This repository contains the code of the inspection triggers that provide distributed controllers with introspective information on the state of the system at runtime.
 
## Requirements

* An OpenStack Swift deployment (this project was tested from Kilo to Pike OpenStack releases).

* A [Crystal controller](https://github.com/Crystal-SDS/controller) deployment.

## Installation

To install the module, clone the repository and run the installation command in the root directory:
```sh
git clone https://github.com/Crystal-SDS/metric-middleware
cd metric-middleware
sudo python setup.py install
```

After that, it is necessary to configure OpenStack Swift to add the middleware to the Proxy and Object servers.


# Proxy

Edit the `/etc/swift/proxy-server.conf` file in each Proxy Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `crystal_filter_handler` filter.

```ini
[pipeline:main]
pipeline = catch_errors gatekeeper healthcheck proxy-logging cache container_sync bulk ratelimit authtoken keystoneauth container-quotas account-quotas crystal_metric_handler crystal_filter_handler slo dlo proxy-logging proxy-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:

```ini
[filter:crystal_metric_handler]
use = egg:swift_crystal_metric_middleware#crystal_metric_handler

#Node Configuration
execution_server = proxy
region_id = 1
zone_id = 1

#RabbitMQ Configuration
rabbit_host = controller
rabbit_port = 5672
rabbit_username = changeme
rabbit_password = changeme

#Reddis Configuration
redis_host = controller
redis_port = 6379
redis_db = 0

```

# Storage Node

Edit the `/etc/swift/object-server.conf` file in each Storage Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `crystal_filter_handler` filter.
```ini
[pipeline:main]
pipeline = healthcheck recon crystal_metric_handler crystal_filter_handler object-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:

```ini
[filter:crystal_metric_handler]
use = egg:swift_crystal_metric_middleware#crystal_metric_handler

#Node Configuration
execution_server = object
region_id = 1
zone_id = 1

#RabbitMQ Configuration
rabbit_host = controller
rabbit_port = 5672
rabbit_username = changeme
rabbit_password = changeme

#Reddis Configuration
redis_host = controller
redis_port = 6379
redis_db = 0
```

The last step is to restart the proxy-server/object-server services:
```bash
sudo swift-init proxy restart
sudo swift-init object restart
```

## Usage

Crystal Metric Middleware is an extension point of Crystal: new metrics can be added in order to provide more information to the controller. 
This repository includes some [metric samples](/metric_samples) like the number of GET/PUT operations, the number of active operations, the bandwidth used or the request performance.

The code below is an example of a simple metric that counts GET requests:

```python
from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric

# The metric class inherits from AbstractMetric
class GetOps(AbstractMetric):
    
    # This method must be implemented because is the main interception point.
    def execute(self):
        # Setting the type of the metric. The metric internal value is reset when it is 
        # published (at periodic intervals) 
        self.type = 'stateless'
        
        # Checking if it's a GET operation and an object request
        if self.method == "GET" and self._is_object_request():
            # register_metric(key, value) increments by one the current metric (GetOps) 
            # for the current target tenant
            self.register_metric(self.account, 1)

        return self.response
```

The code below is an example of a bandwidth metric for GET object requests:

```python
class GetBw(AbstractMetric):
    
    def execute(self):
        self.type = 'stateless'
        
        if self.method == "GET" and self._is_object_request():
            # By calling _intercept_get(), all read chunks will enter on_read method 
            self._intercept_get()
            
        # If the request is intercepted with _intercept_get() it is necessary to return the response          
        return self.response
 
    def on_read(self, chunk):
        # In this case, the metric counts the number of bytes
        mbytes = (len(chunk)/1024.0)/1024
        self.register_metric(self.account, mbytes)
```

The next example shows a metric that counts active PUT requests:

```python
class PutActiveRequests(AbstractMetric):

    def execute(self):
        # The type is stateful: the internal value is never reset
        self.type = 'stateful'
        
        if self.method == "PUT" and self._is_object_request():
            self._intercept_put()
            # Incrementing the metric (a new active PUT request has been intercepted)
            self.register_metric(self.account, 1)

        return self.request

    def on_finish(self):
        # Decrementing the metric (the PUT request has just finished)
        self.register_metric(self.account, -1)
```

Metric classes (inheriting from AbstractMetric) must implement the `execute()` method and can implement the optional `on_read()` and `on_finish()` methods.

* `execute()`: This method is the main interception point. If the metric needs access to the data flow or needs to know when the request has finished, it has to call `self._intercept_get()` or `self._intercept_put()`.
 
* `on_read()`: this method is called if the metric has previously called `self._intercept_get()` or `self._intercept_put()`. All read chunks will enter this method. 

* `on_finish()`: this method is called if the metric has previously called `self._intercept_get()` or `self._intercept_put()`. This method is called once the request has been completed.

There are three types of metrics supported:

* `stateless`: the default type. When the metric is published (typically at one second intervals), the internal value is reset to 0.

* `stateful`: when the metric is published, the internal value is not reset. Thus, the value can be incremented/decremented during the next intervals. In a previous example, a stateful metric is used to count the active PUT requests: whenever a request is intercepted the metric value is incremented, and when the request finishes the metric value is decremented. At periodic intervals the metric value is published, showing how many concurrent requests are being served at a given instant.  

* `force`: this type of metric is published directly after the call to register_metric instead of being published at intervals.  

Metric classes must be registered and enabled through [Crystal controller API](https://github.com/Crystal-SDS/controller/). 
A convenient [web dashboard](https://github.com/iostackproject/SDS-dashboard) is also available to simplify these API calls.

## Support

Please [open an issue](https://github.com/Crystal-SDS/metric-middleware/issues/new) for support.

## Contributing

Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/Crystal-SDS/metric-middleware/compare/).
