from distutils.core import setup

setup(name='veritable',
	  version='0.1',
	  description='Python Client Library for Veritable API',
	  author='Prior Knowledge, Inc.',
	  author_email='developers@priorknowledge.com',
	  maintainer='Max Gasner',
	  maintainer_email='max@priorknowledge.com',
	  url='http://www.priorknowledge.com/',
	  requires=['requests', 'simplejson'],
	  packages=['veritable'])
	  