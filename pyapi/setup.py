import sys

from distutils.core import setup, Extension

if sys.platform == 'darwin':
    extra_compile_args = ['-fnested-functions']
else:
    extra_compile_args = []

discodb_module = Extension('discodb._discodb',
                           sources=['src/discodbmodule.c',
                                    '../ddb.c',
                                    '../ddb_cons.c',
                                    '../ddb_cnf.c',
                                    '../ddb_valuemap.c',
                                    '../util.c'],
                           include_dirs=['..'],
                           libraries=['cmph'],
                           extra_compile_args=extra_compile_args,)

setup(name='discodb',
      version='0.1',
      url='',
      description='An efficient, immutable, persistent mapping object.',
      author='Nokia Research Center',
      author_email='',
      ext_modules=[discodb_module],
      packages=['discodb'],
      package_dir={'discodb': 'src/discodb'})
