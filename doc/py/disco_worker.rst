
:mod:`disco_worker` --- Runtime environment for Disco jobs
==========================================================

.. module:: disco_worker
   :synopsis: Runtime environment for Disco jobs
   
Disco master runs :mod:`disco_worker` to execute map and reduce functions
for a Disco job. The module contains several classes and functions that
are used by Disco internally to connect to input sources, write output
files etc. However, this page only documents features that may be useful
for writing new Disco jobs.

As job functions are imported to the :mod:`disco_worker` namespace
for execution, they can use functions in this module directly without
importing the module explicitely.

.. function:: msg(message)

Sends the string *message* to the master for logging. The message is
shown on the web interface. To prevent a rogue job from overwhelming the
master, the maximum *message* size is set to 255 characters and job is
allowed to send at most 10 messages per second.

.. function:: err(message)

Raises an exception with the reason *message*. This terminates the job.

.. function:: data_err(message)

Raises a data error with the reason *message*. This signals the master to re-run
the task on another node. If the same task raises data error on several
different nodes, the master terminates the job. Thus data error should only be
raised if it is likely that the occurred error is temporary.

Typically this function is used by map readers to signal a temporary failure
in accessing an input file.


