.. _chunking:

Pushing Chunked Data to DDFS
============================

::

        ddfs chunk data:bigtxt /path/to/bigfile.txt

.. hint:: If the local ``/path/to/bigfile.txt`` is in the current directory,
          you must use ``./bigfile.txt``, or another path containing `/` chars,
          in order to specify it.
          Otherwise :mod:`ddfs <ddfscli>` will think you are specifying a :ref:`tag <tags>`.
