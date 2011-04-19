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
