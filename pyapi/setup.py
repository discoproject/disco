from distutils.core import setup, Extension

discodb_module = Extension('discodb',
                           sources=['discodbmodule.c'],
                           include_dirs=['..'],
                           libraries=['cmph', 'judy'])

setup(name='discodb',
      version='0.1',
      url='',
      description='An efficient, immutable, persistent mapping object.',
      author='Nokia Research Center',
      author_email='',
      ext_modules=[discodb_module])
