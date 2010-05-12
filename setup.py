import os, sys

from distutils.core import setup

home     = os.path.dirname(os.path.realpath(__file__))
packages = os.listdir(os.path.join(home, 'lib'))

setup(name='discodex',
      version='0.1',
      description='intuitive data indexing',
      author='Nokia Research Center',
      package_dir={'': 'lib'},
      packages=packages)
