
:mod:`discoapi` --- Low-level Disco API
=======================================

.. module:: discoapi
   :synopsis: Low-level inteface to the Disco's Web API

The :mod:`discoapi` provides a low-level interface for manipulating and
querying status of jobs and the Disco master. Disco provides a REST-style
Web API for accessing the master, for which the :class:`discoapi.Disco`
object provides a simple Python interface.

This module also provides a simple command-line interface for the methods
in :class:`discoapi.Disco`. The syntax is the following

``python discoapi.py <master> <method> [parameters]``

For instance, you can kill the job ``test_simple@1215407370`` as follows:

``python discoapi.py disco://localhost:5500 kill test_simple@1215407370``

Note that this module depends on the modules *cjson* and *netstring* that 
are not part of the standard Python distribution.

.. class:: JobException

   Raised when job fails on Disco master.

   .. attribute:: msg
 
   Error message.

   .. attribute:: JobException.name

   Name of the failed job.

   .. attribute:: JobException.master
   
   Address of the Disco master that produced the error.

.. class:: Disco(host)

   Encapsulates connection to the Disco master.

   *host* is the address of the Disco master, for instance
   ``disco://localhost:5000``.

   .. method:: Disco.request(url[, data, raw_handle])

   Requests *url* at the master. If a string *data* is specified, a POST request
   is made with *data* as the request payload. If *raw_handle* is set to *True*,
   a file handle to the results is returned. By default a string is returned
   that contains the reply for the request. This method is mostly used by other
   methods of this class internally.

   .. method:: Disco.nodeinfo()

   Returns a dictionary describing status of nodes managed by the master.
   
   .. method:: Disco.joblist()

   Returns a list of jobs and their status from the master.

   .. method:: Disco.kill(name)

   Kills the job *name*.

   .. method:: Disco.clean(name)

   Cleans records of the job *name*. Note that after the job records have been
   cleaned, there is no way to obtain addresses to the result files from the
   master, although no files are deleted by :meth:`Disco.clean`.

   .. method:: Disco.jobspec(name)

   Returns the job request, as specified by :func:`disco.job` for
   the job *name*.

   .. method:: Disco.results(name)

   Returns the list of result files for the job *name*, if available.

   .. method:: Disco.jobinfo(name)

   Returns a dictionary containing information about the job *name*.

   .. method:: Disco.wait(name[, poll_interval, timeout])

   Block until the job *name* has finished. Polls the server for the job status
   every *poll_interval* seconds. Raises a :class:`discoapi.JobException` if the
   job hasn't finished in *timeout* seconds, if specified.





   



