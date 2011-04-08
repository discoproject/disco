.. _worker_protocol:

===========================
The Worker Message Protocol
===========================

.. note:: This protocol is needed to create a :term:`worker` for Disco.
          You don't need to write your own worker to run a :term:`job`,
          you can usually use the standard :mod:`disco.worker`.

A :term:`worker` is an executable that can run a task,
given by the Disco :term:`master`.
The executable could either be a binary executable,
or a script that launches other executables.
In order to be usable as a Disco worker,
an executable needs to implement the :ref:`worker_protocol`.

There are two parts to using a binary executable as a Disco worker.
The first part is the submission of the executable to the Disco master.
The second part is the interaction of the executable with its
execution environment, when running a :term:`task`.

This document describes how the worker should communicate with the master,
while it is executing.
For more on submitting the :term:`job pack`, see :ref:`jobpack`.

.. note:: The same executable is launched to perform :term:`map` and :term:`reduce` tasks.

Message Format
==============

The worker sends messages over :term:`stderr` to the master,
and receives responses over :term:`stdin`.
The master should respond within *600 seconds*:
it is advised for the worker to timeout after waiting this long.

.. note:: Workers should not write anything to :term:`stderr`,
          except messages formatted as described here.
          :term:`stdout` is also initially redirected to stderr.
          The :term:`worker` uses stderr to send signals to the :term:`master`.

.. note:: In Python, :class:`Workers <disco.worker.Worker>` wrap all exceptions,
          and everything written to stdout,
          with an appropriate message to the master (on stderr).
          For instance, you can raise a :class:`disco.error.DataError`
          to abort the worker and try again on another host.
          For other types of failures,
          it is usually best to just let the worker catch the exception.

Messages to the master look like::

         **<type:version> timestamp tags
         payload
         <>**

where type is one of:

      |     :ref:`DAT`
      |     :ref:`END`
      |     :ref:`ERR`
      |     :ref:`INP`
      |     :ref:`JOB`
      |     :ref:`MSG`
      |     :ref:`OUT`
      |     :ref:`PID`
      |     :ref:`SET`
      |     :ref:`STA`
      |     :ref:`TSK`
      |     :ref:`VSN`

version is 2 bytes

timestamp is::

          %y/%m/%d %H:%M:%S

tags are::

     (^\w+ ?)*

Valid Messages
==============

.. _VSN:

VSN
---

   Announce the version of the message protocol the worker is using.

   The worker should send a *VSN* message before it sends any others.
   The payload of the VSN message should be the protocol version the worker is using
   The current version is `"0.1"`.
   The master should respond `'OK'` if it intends to use the same version.

.. _PID:

PID
---

   Announce the :term:`pid` to the master.
   The worker should send this so it can be properly killed,
   (e.g. if there's a problem with the :term:`job`).

   The worker should send a *PID* message,
   with a payload containing its :term:`pid` as a string.
   The master should respond `'OK'`.

.. _JOB:

JOB
---

   Request the location of the jobpack from the master.

   The worker can send a *JOB* message with no payload.
   The master should respond with a `'JOB'` message,
   and a payload containing the path of the :term:`job pack` as a string.

.. _TSK:

TSK
---

   Request the task info from the master.

   The worker should send a *TSK* message with no payload.
   The master should respond with a `'TSK'` message,
   and a payload containing the task info as a dictionary.

   "host"
        The host the :term:`task` is running on.

   "jobname"
        The name of the :term:`job`.

   "master"
        The host the :term:`master` is running on.

   "mode"
        The mode or phase of the :term:`job`.
        Either `"map"` or `"reduce"`.

   "taskid"
        The internal Disco id of the :term:`task`.


.. _INP:

INP
---
   Request input from the master.

   The worker can send an *INP* message, with no payload.
   The master should respond with an `'INP'` message,
   and a payload containing a two-element tuple (list in :term:`JSON`).

   The first element is a flag,
   which will either be `'more'` or `'done'`,
   indicating whether the worker should continue to poll for new inputs.

   The second element is a list of inputs.
   Each input is a three-element tuple::

           input_id, status, replicas

   where::

           status ::= 'ok' | 'busy' | 'failed'
           replicas ::= [replica]
           replica ::= rep_id, replica_location

   The worker can also send an *INP* message,
   with a payload containing an `input_id`.
   The master should respond with an `'INP'` message,
   and a payload containing a two-element tuple::

         status, replicas

.. _MSG:

MSG
---

   Send a message (i.e. to be displayed in the ui).

   The worker can send a *MSG* message, with a payload containing a string.
   The master should respond `'OK'`.

.. _SET:

SET
---

   Request the system settings.

   The worker can send a *SET* message.
   The master should respond with a `'SET'` message,
   and a payload containing the settings as a dictionary.

   "port"
        The port the Disco master is running on.

   "put_port"
        The put port used for DDFS.

.. _STA:

STA
---

   Send a status message to the master.
   Status messages are used to log the status of the worker.

   The worker can send a *STA* message, with a payload containing a string.
   The master should respond `'OK'`.

.. _OUT:

OUT
---
   The worker should report its output to the master.

.. _END:

END
---

   Inform the master that the worker is finished.

   The worker should only send this message after syncing all output files,
   since Disco normally terminates the worker when this message is received.
   The worker should not exit immediately after sending this message,
   since there is no guarantee if the message will be received by the
   master if the worker exits.
   Instead, the worker should wait for the response from the master
   (as it should for all messages).

   The worker should send an *END* message, with no payload.
   The master should respond `'OK'`.

.. _DAT:

DAT
---

   Report a failed input or transient error to the master.

   The worker can send a *DAT* message,
   with a payload containing the error message as a string.
   The master should respont `'OK'`.

.. _ERR:

ERR
---

   Report a fatal error to the master.

   The worker can send an *ERR* message,
   with a payload containig the error message as a string.
   The master should respond `'OK'`.
