.. _worker_protocol:

=========================
The Disco Worker Protocol
=========================

.. note:: This protocol is used by a :term:`worker` in Disco.
          You don't need to write your own worker to run a :term:`job`,
          you can usually use the standard :mod:`disco.worker`.

A :term:`worker` is an executable that can run a task, given by the
Disco :term:`master`.  The executable could either be a binary
executable, or a script that launches other executables.  In order to
be usable as a Disco worker, an executable needs to implement the
:ref:`worker_protocol`. For instance, `ODisco
<https://github.com/pmundkur/odisco>`_, which implements the Disco
Worker Protocol, allows you to write Disco jobs in
`the O'Caml language <http://caml.inria.fr/ocaml/index.en.html>`_.

There are two parts to using a binary executable as a Disco worker.
The first part is the submission of the executable to the Disco master.
The second part is the interaction of the executable with its
execution environment, when running a :term:`task`.

This document describes how the worker should communicate with
Disco, while it is executing.  For more on creating and submitting
the :term:`job pack`, see :ref:`jobpack`.

.. note:: The same executable is launched to perform :term:`map` and
          :term:`reduce` tasks.

A worker-initiated synchronous message-based protocol is used for this
communication.  That is, all communication is initiated by the worker
by sending a message to Disco, and Disco in turn sends a reply to each
message.  A worker could pipeline several messages to Disco, and
it will receive a response to each message in order.

The worker sends messages over :term:`stderr` to Disco, and
receives responses over :term:`stdin`.  Disco should respond
within *600 seconds*: it is advised for the worker to timeout after
waiting this long.

.. note:: Workers should not write anything to :term:`stderr`,
          except messages formatted as described below.
          :term:`stdout` is also initially redirected to stderr.

.. note:: In Python, :class:`Workers <disco.worker.Worker>` wrap all
          exceptions, and everything written to :term:`stdout`, with
          an appropriate message to Disco (on :term:`stderr`).
          For instance, you can raise a :class:`disco.error.DataError`
          to abort the worker and try again on another host.  For
          other types of failures, it is usually best to just let the
          worker catch the exception.

Message Format
==============

Messages in the protocol, both from and to the worker, are in the format:

         *<name>* 'SP' *<payload-len>* 'SP' *<payload>* '\\n'

where 'SP' denotes a single space character, and *<name>* is one of:

      |     :ref:`DONE`
      |     :ref:`ERROR`
      |     :ref:`FAIL`
      |     :ref:`FATAL`
      |     :ref:`INPUT`
      |     :ref:`INPUT_ERR`
      |     :ref:`MSG`
      |     :ref:`OK`
      |     :ref:`OUTPUT`
      |     :ref:`PING`
      |     :ref:`RETRY`
      |     :ref:`TASK`
      |     :ref:`WAIT`
      |     :ref:`WORKER`

*<payload-len>* is the length of the *<payload>* in bytes,
and *<payload>* is a :term:`JSON` formatted term.

Note that messages that have no payload (see below) actually
contain an empty JSON string *<payload> = ""* and *<payload-len> = 2*.

Messages from the Worker to Disco
=================================

.. _WORKER:

WORKER
------

   Announce the startup of the worker.

   The payload is a dictionary containing the following information:

   "version"
        The version of the message protocol the worker is using, as a
        string.  The current version is `"1.0"`.

   "pid"
        The integer :term:`pid` of the worker.

        The worker should send this so it can be properly killed,
        (e.g. if there's a problem with the :term:`job`).  This is
        currently required due to limitations in the Erlang support
        for external spawned processes.

   The worker should send a `WORKER` message before it sends any
   others.  Disco should respond with an `OK` if it intends to use the
   same version.

.. _TASK:

TASK
----

   Request the task information from Disco.

   The worker should send a `TASK` message with no payload.  Disco
   should respond with a `TASK` message, and a payload containing the
   following task information as a dictionary:

   "host"
        The host the :term:`task` is running on.

   "master"
        The host the :term:`master` is running on.

   "jobname"
        The name of the :term:`job` this task is a member of.

   "taskid"
        The internal Disco id of the :term:`task`.

   "mode"
        The mode or phase of the :term:`job`.  This is currently
        either `"map"` or `"reduce"`, although more modes may be added
        in future releases.

   "disco_port"
        The value of the :envvar:`DISCO_PORT` setting, which is the
        port the Disco master is running on, and the port used to
        retrieve data from Disco and :ref:`DDFS <ddfs>`.  This is used
        to convert URLs with the `disco` and `ddfs` schemes into
        `http` URLs.

   "put_port"
        The value of the :envvar:`DDFS_PUT_PORT` setting.  This can
        be used by the worker to upload results to :ref:`DDFS <ddfs>`.

   "disco_data"
        The value of the :envvar:`DISCO_DATA` setting.

   "ddfs_data"
        The value of the :envvar:`DDFS_DATA` setting.  This can be
        used to read :ref:`DDFS <ddfs>` data directly from the local
        filesystem after it has been ascertained that the :ref:`DDFS
        <ddfs>` data is indeed local to the current host.

   "jobfile"
        The path to the :ref:`jobpack` file for the current job.  This
        can be used to access any :ref:`jobdata` that was uploaded as
        part of the :ref:`jobpack`.

.. _INPUT:

INPUT
-----
   Request input for the task from Disco.

   To get the complete list of current inputs for the task, the worker
   can send an `INPUT` message with no payload.  Disco should
   respond with an `INPUT` message, and a payload containing a
   two-element tuple (list in :term:`JSON`).

   The first element is a flag, which will either be `'more'` or
   `'done'`.  `'done'` indicates that the input list is complete,
   while `'more'` indicates that more inputs could be added to the
   list in the future, and the worker should continue to poll for new
   inputs.

   The second element is a list of inputs, where each input is a
   specified as a three-element tuple::

           input_id, status, replicas

   where `input_id` is an integer identifying the input, and `status`
   and `replicas` follow the format::

           status ::= 'ok' | 'busy' | 'failed'
           replicas ::= [replica]
           replica ::= rep_id, replica_location

   It is possible for an input to be available at multiple locations;
   each such location is called a `replica`.  A `rep_id` is an integer
   identifying the replica.

   The `replica_location` is specified as a URL. The protocol scheme
   used for the `replica_location` could be one of `http`, `disco`,
   `dir` or `raw`. A URL with the `disco` scheme is to be accessed using
   HTTP at the `disco_port` specified in the `TASK` response from Disco.
   The `raw` scheme denotes that the URL itself (minus the scheme) is
   the data for the task. The data needs to be properly URL encoded,
   for instance using Base64 encoding. The `dir` is like the `disco`
   scheme, except that the file pointed to contains lines of the form

   *<label>* 'SP' *<url>* '\\n'

   The `'label'` comes from the `'label'` specified in an `OUTPUT`
   message by a task, while the `'url'` points to a file containing
   output data generated with that label.  This is currently how
   labeled output data is communicated by upstream tasks to downstream
   ones, e.g. from map tasks to reduce tasks, or from tasks in the
   final phase of a previous job to the tasks in the first phase of a
   subsequent job (see :ref:`dataflow`).

   One important optimization is to use the local filesystem instead
   of HTTP for accessing inputs when they are local.  This can be
   determined by comparing the URL hostname with the `host` specified
   in the `TASK` response, and then converting the URL path into a
   filesystem path using the `disco_data` or `ddfs_data` path prefixes
   for URL paths beginning with `disco/` and `ddfs/` respectively.

   The common input status will be `'ok'` - this indicates that as far
   as Disco is aware, the input should be accessible from at
   least one of the specified replica locations.  The `'failed'`
   status indicates that Disco thinks that the specified
   locations are inaccessible; however, the worker can still choose to
   ignore this status and attempt retrieval from the specified
   locations.  A `'busy'` status indicates that Disco is in the
   process of generating more replicas for this input, and the worker
   should poll for additional replicas if needed.

   It is recommended that the worker attempts the retrieval of an
   input from the replica locations in the order specified in the
   response.  That is, it should attempt retrieval from the first
   replica, and if that fails, then try the second replica location,
   and so on.

   When a worker polls for any changes in task's input, it is
   preferable not to repeatedly retrieve information for inputs
   already successfully processed.  In this case, the worker can send
   an `INPUT` message with an `'exclude'` payload that specifies the
   `input_ids` to exclude in the response.  In this case, the `INPUT`
   message from the worker should have the following payload::

           ['exclude', [input_id]]

   On the other hand, when a worker is interested in changes in
   replicas for a particular set of inputs, it can send an `INPUT`
   message with an `include` payload that requests information only
   for the specified `input_ids`.  The `INPUT` message from the worker
   in this case should have the following payload::

           ['include', [input_id]]

.. _INPUT_ERR:

INPUT_ERR
---------

   Inform Disco that about failures in retrieving inputs.

   The worker should inform Disco if it cannot retrieve an input due
   to failures accessing the replicas specified by Disco in the
   `INPUT` response.  The payload of this message specifies the input
   and the failed replica locations using their identifiers, as
   follows::

           [input_id, [rep_id]]

   If there are alternative replicas that the worker can try, Disco
   should respond with a `RETRY` message, with a payload specifying new
   replicas::

           [[rep_id, replica_location]]

   If there are no alternatives, and it is not possible for Disco to
   generate new alternatives, Disco should reply with a `FAIL` message
   (which has no payload).

   If Disco is in the process of generating new replicas, it should
   reply with a `WAIT` message and specify an integer duration in
   seconds in the payload.  The worker should then poll for any new
   replicas after the specified duration.

.. _MSG:

MSG
---

   Send a message (i.e. to be displayed in the ui).

   The worker can send a `MSG` message, with a payload containing a string.
   Disco should respond with an `OK`.


.. _OUTPUT:

OUTPUT
------

   The worker should report its output(s) to Disco.

   For each output generated by the worker, it should send an `OUTPUT`
   message specifying the type and location of the output, and
   optionally, its label::

      [output_location, output_type, label]

   The `output_type` can be either `'disco'`, `'part'` or `'tag'`.
   `'disco'` and `'part'` outputs are used for local outputs, while
   `'tag'` specifies a location within :ref:`DDFS <ddfs>`.

   Local outputs have locations that are paths relative to `jobhome`.

   Labels are currently only interpreted for `'part'` outputs, and are
   integers that are used to denote the partition for the output.

.. _DONE:

DONE
----

   Inform Disco that the worker is finished.

   The worker should only send this message (which has no payload)
   after syncing all output files, since Disco normally terminates the
   worker when this message is received.  The worker should not exit
   immediately after sending this message, since there is no guarantee
   if the message will be received by Disco if the worker exits.
   Instead, the worker should wait for the response from Disco
   (as it should for all messages).

.. _ERROR:

ERROR
-----

   Report a failed input or transient error to Disco.

   The worker can send a `ERROR` message with a payload containing the
   error message as a string.  This message will terminate the worker,
   but not the job.  The current task will be retried by Disco.  See
   also the information above for the `DONE` message.

.. _FATAL:

FATAL
-----

   Report a fatal error to the master.

   The worker can send an `FATAL` message, with a payload containig
   the error message as a string.  This message will terminate the
   entire job.  See also the information above for the `DONE` message.

.. _PING:

PING
----

   No-op - always returns `OK`.

   Worker can use `PING` as a heartbeat message, to make sure that the
   master is still alive and responsive.


Messages from Disco to the Worker
=================================

.. _OK:

OK
--

   A generic response from Disco.  This message has the payload `"ok"`.

.. _FAIL:

FAIL
----

   A possible response from Disco for an `INPUT_ERR` message, as described above.

.. _RETRY:

RETRY
-----

   A possible response from Disco for an `INPUT_ERR` message, as described above.

.. _WAIT:

WAIT
-----

   A possible response from Disco for an `INPUT_ERR` message, as described above.

.. _protocol_session:

Sessions of the Protocol
========================

On startup, the worker should first send the `WORKER` message, and
then request the `TASK` information.  The `taskid` and `mode` in
the `TASK` response can be used, along with the current system time,
to create a working directory within which to store any scratch data
that will not interact with other, possibly concurrent, workers
computing other tasks of the same job.  These messages can be said to
constitute the initial handshake of the protocol.

The crucial messages the worker will then send are the `INPUT` and
`OUTPUT` messages, and often the `INPUT_ERR` messages.  The processing
of the responses to `INPUT` and `INPUT_ERR` will be determined by the
application.  The worker will usually end a successful session with
one or more `OUTPUT` messages followed by the `DONE` message.  Note
that it is possible for a successful session to have several
`INPUT_ERR` messages, due to transient network conditions in the
cluster as well as machines going down and recovering.

An unsuccessful session is normally ended with an `ERROR` or `FATAL`
message from the worker.  An `ERROR` message terminates the worker,
but does not terminate the job; the task will possibly be retried on
another host in the cluster.  A `FATAL` message, however, terminates
both the worker, and the entire job.

.. _new_worker:

Considerations when implementing a new Worker
=============================================

You will need some simple and usually readily available tools when
writing a new worker that implements the Disco protocol.  Parsing and
generating messages in the protocol requires a :term:`JSON` parser and
generator.  Fetching data from the replica locations specified in the
Disco `INPUT` responses will need an implementation of a simple HTTP
client.  This HTTP client and :term:`JSON` tool can also be used to
persistently store computed results in :ref:`DDFS <ddfs>` using the
REST API.

The protocol does not specify the data contents of the Disco job
inputs or outputs.  This leaves the implementor freedom to choose an
appropriate format for marshalling and parsing data output by the
worker tasks.  This choice will have an impact on how efficiently the
computation is performed, and how much disk space the marshalled
output uses.
