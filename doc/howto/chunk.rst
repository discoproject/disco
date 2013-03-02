.. _chunking:

Pushing Chunked Data to DDFS
============================

::

        ddfs chunk data:bigtxt /path/to/bigfile.txt

.. hint:: If the local ``/path/to/bigfile.txt`` is in the current directory,
          you must use ``./bigfile.txt``, or another path containing `/` chars,
          in order to specify it.
          Otherwise :mod:`ddfs <ddfscli>` will think you are specifying a :ref:`tag <tags>`.

The creation of chunks is record-aware; i.e. chunks will be created on
record boundaries, and 'ddfs chunk' will not split a single record
across separate chunks.  The default record parser breaks records on
line boundaries; you can specify your own record parser using the
``reader`` argument to the ``ddfs.chunk`` function, or the ``-R``
argument to ``ddfs chunk``.

The chunked data in DDFS is stored in Disco's internal format, which
means that when you read chunked data in your job, you will need to
use the ``disco.worker.task_io.chain_reader``.  Hence, as is typical, if your
map tasks are reading chunked data, specify
``map_reader=disco.worker.task_io.chain_reader`` in your job.
