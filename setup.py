from setuptools import setup, find_packages

paste_factory = ['crystal_metric_middleware = '
                 'crystal_metric_middleware.crystal_metric_handler:filter_factory']

setup(name='swift_crystal_metric_middleware',
      version='0.0.3',
      description='Crystal metric middleware for OpenStack Swift',
      author='The AST-IOStack Team: Josep Sampe, Raul Gracia',
      url='http://iostack.eu',
      packages=find_packages(),
      requires=['swift(>=1.4)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
