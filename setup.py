from distutils.core import setup
from veritable.version import __version__

setup(name='veritable',
	  version=__version__,
	  description='Python Client Library for Veritable API',
	  author='Prior Knowledge, Inc.',
	  author_email='developers@priorknowledge.com',
	  maintainer='Max Gasner',
	  maintainer_email='max@priorknowledge.com',
	  url='http://www.priorknowledge.com/',
	  install_requires=['requests', 'simplejson'],
	  packages=['veritable'])
