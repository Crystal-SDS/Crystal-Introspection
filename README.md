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

### Proxy

Edit the `/etc/swift/proxy-server.conf` file in each Proxy Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `crystal_filters` filter.

```ini
[pipeline:main]
pipeline = catch_errors gatekeeper healthcheck proxy-logging cache container_sync bulk ratelimit authtoken crystal_acl keystoneauth container-quotas account-quotas crystal_metrics crystal_filters copy slo dlo proxy-logging proxy-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:

```ini
[filter:crystal_metrics]
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

### Storage Node

Edit the `/etc/swift/object-server.conf` file in each Storage Node, and perform the following changes:

1. Add the Crystal Metric Middleware to the pipeline variable. This filter must be added before the `crystal_filters` filter.
```ini
[pipeline:main]
pipeline = healthcheck recon crystal_metrics crystal_filters object-server

```

2. Add the configuration of the filter. Copy the lines below to the bottom part of the file:

```ini
[filter:crystal_metrics]
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

Metric classes can implement the `on_start()`, `on_read()` and `on_finish()` methods.

* `on_start()`: This method is called when the requests starts, only once.
 
* `on_read(chunk)`: this method is called each time a chunk is read from the main stream. The read chunk enters to the method in order to be able to operate over it. 

* `on_finish()`: This method is called when the requests finishes, only once.

There are three types of metrics supported:

* `stateless`: the default type. When the metric value is published, the metric value is reset to 0.

* `stateful`: when the metric is published, the value is not reset. Thus, the metric value can be incremented/decremented during the next intervals. In an below example, a stateful metric is used to count the active requests.  

* `force`: this type of metric is published directly after the call to register_metric instead of being published at intervals.  

Metric classes must be registered and enabled through [Crystal controller API](https://github.com/Crystal-SDS/controller/). 
A convenient [web dashboard](https://github.com/Crystal-SDS/dashboard) is also available to simplify these API calls.


## Examples
The code below is an example of a simple metric that counts GET requests per second.:

```python
from crystal_metric_middleware.metrics.abstract_metric import AbstractMetric

# The metric class inherits from AbstractMetric
class OperationsPerSecond(AbstractMetric):

	type = 'stateless'
	
	on_start(self):
		self.register_metric(1)
```

The code below is an example of a bandwidth metric that counts the number of MBytes per second:

```python
class Bandwidth(AbstractMetric):

    type = 'stateless'
    
    def on_read(self, chunk):
        mbytes = (len(chunk)/1024.0)/1024
        self.register_metric(self.account, mbytes)
```

The next example shows a metric that counts active requests: whenever a request is intercepted, the metric value is incremented, and when the request finishes, the metric value is decremented. At periodic intervals the metric value is published, showing how many concurrent requests are being served at a given instant.

```python
class ActiveRequests(AbstractMetric):

    type = 'statefull'
    
    def on_start(self):
    	# Incrementing the metric
    	self.register_metric(+1)

    def on_finish(self):
        # Decrementing the metric
        self.register_metric(-1)
```

## Support

Please [open an issue](https://github.com/Crystal-SDS/metric-middleware/issues/new) for support.

## Contributing

Please contribute using [Github Flow](https://guides.github.com/introduction/flow/). Create a branch, add commits, and [open a pull request](https://github.com/Crystal-SDS/metric-middleware/compare/).
