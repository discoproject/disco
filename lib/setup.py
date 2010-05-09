#!/usr/bin/env python

import os
from distutils.core import setup

lib = os.path.dirname(os.path.realpath(__file__))

def run_setup(libname):
    os.chdir(os.path.join(lib, libname))
    execfile('setup.py')

setup(name='disco',
      version='0.1',
      packages=['disco', 'clx'])

run_setup('discodb')
