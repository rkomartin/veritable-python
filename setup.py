from setuptools import setup

setup(name='veritable',
	  version='1.0.0',
	  description='Python Client Library for Veritable API',
	  author='Prior Knowledge, Inc.',
	  author_email='developers@priorknowledge.com',
	  maintainer='Max Gasner',
	  maintainer_email='max@priorknowledge.com',
	  url='http://www.priorknowledge.com/',
	  install_requires=['requests', 'simplejson'],
	  packages=['veritable'])
