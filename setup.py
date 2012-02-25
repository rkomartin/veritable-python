import os
from setuptools import setup

def get_version():
    basedir = os.path.dirname(__file__)
    with open(os.path.join(basedir, 'veritable/version.py')) as f:
        __version__ = None
        exec(f.read())
        return __version__

if __name__ == '__main__':
    setup(name='veritable',
	  version=get_version(),
	  description='Python Client Library for Veritable API',
	  author='Prior Knowledge, Inc.',
	  author_email='developers@priorknowledge.com',
	  maintainer='Max Gasner',
	  maintainer_email='max@priorknowledge.com',
	  url='http://www.priorknowledge.com/',
	  install_requires=['requests', 'simplejson'],
	  packages=['veritable'])
