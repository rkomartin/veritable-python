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
      long_description='Veritable is a system for understanding and making predictions about tabular data -- i.e., structured data that can be organized into rows and columns. Its basic inputs are rows of data, which it uses to learn a joint predictive model. Its outputs are predictions about the values of new rows: given the values of any subset of columns, Veritable can predict the values of any other columns.',
      author='Prior Knowledge, Inc.',
      author_email='developers@priorknowledge.com',
      maintainer='Max Gasner',
      maintainer_email='max@priorknowledge.com',
      url='http://dev.priorknowledge.com/',
      install_requires=['requests'],
      packages=['veritable'],
      platforms=['any'],
      license='MIT',
      classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.1',
        'Topic :: Database',
        'Topic :: Scientific/Engineering :: Information Analysis'
      ])
