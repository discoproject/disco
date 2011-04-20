#!/usr/bin/env python
import os
from distutils.core import setup

def ispackage(path):
    return os.path.isdir(path) and '__init__.py' in os.listdir(path)

def packages(dirpath):
    for name in os.listdir(dirpath):
        path = os.path.join(dirpath, name)
        if ispackage(path):
            yield name
            for subpackage in packages(path):
                yield '%s.%s' % (name, subpackage)

package_dir = os.path.realpath(os.path.dirname(__file__))

setup(name='disco',
      version=os.getenv('DISCO_VERSION'),
      description='An open-source mapreduce framework.',
      url='http://discoproject.org',
      author='Nokia Research Center',
      packages=list(packages(package_dir)))
