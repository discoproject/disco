
:mod:`disco.util` --- Helper functions
======================================

.. module:: disco.util
   :synopsis: Helper functions

This module provides utility functions that are mostly used by Disco
internally.

The :func:`external` function below comes in handy if you use the Disco
external interface.

.. function:: disco_host(addr)

   Communication in Disco takes place over HTTP. However, the Disco master
   and Disco nodes seldom run on the default HTTP port, 80, which requires
   that the destination port must be specified explictely in the address,
   for instance ``http://localhost:8989``.

   To make the addresses easier to read, and to enable consistent
   addressing even though the Disco port may vary, a special protocol
   specifier ``disco://`` is provided.

   The ``disco://`` specifier translates to ``http://address:DISCO_PORT``
   where ``DISCO_PORT`` is either specified as an environment variable
   or read from ``/etc/disco/disco.conf``. If neither is available,
   the default value `8989` is used. 

   This function performs the above translation. It is given an address
   *addr*.  If the address starts with ``disco://``, a corresponding
   ``http://`` address is returned. If the address starts with ``http://``
   it is returned as such.


.. function:: jobname(addr)

   Extracts the job name from an address *addr*. This function is
   particularly useful for using the methods in :class:`disco.core.Disco`
   given only results of a job.


.. function:: external(files)

   Packages an external program, together with other files it depends
   on, to be used either as a map or reduce function. *Files* must be
   a list of paths to files so that the first file points at the actual
   executable.
   
   This example shows how to use an external program, *cmap* that needs a
   configuration file *cmap.conf*, as the map function::

        disco.new_job(input = ["disco://localhost/myjob/file1"],
                      fun_map = disco.external(
                           ["/home/john/bin/cmap", "/home/john/cmap.conf"]))

   All files listed in *files* are copied to the same directory so any file
   hierarchy is lost between the files. For more information, see :ref:`discoext`.


.. function:: parse_dir(dir_url[, proxy])

    Translates a directory URL *dir_url*, such as
    ``dir://nx02/test_simple@12243344`` to a list of normal URLs. This
    function might be useful for other programs that need to parse
    results returned by :meth:`disco.core.Disco.wait`, for instance.

    If *proxy* contains address of a proxy server, typically the Disco
    master's, the addresses are translated to point at the proxy, and
    prefixed with ``/disco/node/``. The address of a proxy server is
    typically specified in the environment variable ``DISCO_PROXY``.




