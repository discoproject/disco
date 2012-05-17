"""
:mod:`disco.schemes` -- Default input streams for URL schemes
=============================================================

By default, Disco looks at an input URL and extracts its scheme in order to figure out which input stream to use.

When Disco determines the URL scheme, it tries to import the name `input_stream` from `disco.schemes.scheme_[SCHEME]`, where `[SCHEME]` is replaced by the scheme identified.
For instance, an input URL of `http://discoproject.org` would import and use :func:`disco.schemes.scheme_http.input_stream`.

.. automodule:: disco.schemes.scheme_disco
   :members:

.. automodule:: disco.schemes.scheme_discodb
   :members:

.. automodule:: disco.schemes.scheme_file
   :members:

.. automodule:: disco.schemes.scheme_http
   :members:

.. automodule:: disco.schemes.scheme_raw
   :members:
"""
from disco import util

def import_scheme(url):
    scheme, _rest = util.schemesplit(url)
    scheme = 'scheme_%s' % (scheme or 'file')
    return __import__('disco.schemes.%s' % scheme, fromlist=[scheme])

def input_stream(stream, size, url, params, globals=globals()):
    input_stream = import_scheme(url).input_stream
    util.globalize(input_stream, globals)
    return input_stream(stream, size, url, params)

def open(url, task=None):
    return import_scheme(url).open(url, task=task)

def open_chain(url, task=None):
    from disco.worker.classic.func import chain_reader
    return chain_reader(open(url, task=task), None, url)
