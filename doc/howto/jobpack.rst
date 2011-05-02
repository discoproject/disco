.. _jobpack:

The Job Pack
============

The :term:`job pack` contains all the information needed for creating
and running a Disco :term:`job`.

The first time any :term:`task` of a job executes on a Disco node,
the job pack for the job is retrieved from the master,
and :ref:`jobhome` is unzipped into a job-specific directory.

.. seealso:: The Python :class:`disco.job.JobPack` class.

File format::

        +---------------- 4
        | magic / version |
        |---------------- 8 -------------- 12 ------------- 16 ------------- 20
        | jobdict offset  | jobenvs offset | jobhome offset | jobdata offset |
        |------------------------------------------------------------------ 128
        |                           ... reserved ...                         |
        |--------------------------------------------------------------------|
        |                               jobdict                              |
        |--------------------------------------------------------------------|
        |                               jobenvs                              |
        |--------------------------------------------------------------------|
        |                               jobhome                              |
        |--------------------------------------------------------------------|
        |                               jobdata                              |
        +--------------------------------------------------------------------+

.. _jobdict:

The Job Dict
------------

The :term:`job dict` is a :term:`JSON` dictionary.

    .. attribute:: jobdict.input

       A list of urls or a list of lists of urls.
       Each url is a string.

       .. note::
              An inner list of urls gives replica urls for the same data.
              This lets you specify redundant versions of an input file.
              If a list of redundant inputs is specified,
              the scheduler chooses the input that is located on the node
              with the lowest load at the time of scheduling.
              Redundant inputs are tried one by one until the task succeeds.
              Redundant inputs require that :attr:`map?` is specified.

    .. attribute:: jobdict.worker

       The path to the :term:`worker` binary, relative to the :term:`job home`.
       The master will execute this binary after it unpacks it from :ref:`jobhome`.

    .. attribute:: jobdict.map?

       Boolean telling whether or not this job should have a :term:`map` phase.

    .. attribute:: jobdict.reduce?

       Boolean telling whether or not this job should have a :term:`reduce` phase.

    .. attribute:: jobdict.nr_reduces

       Non-negative integer telling the master how many reduces to run.

       .. warning:: This attribute will soon be removed,
                    as the number of reduces can be inferred in all cases.

    .. attribute:: jobdict.prefix

       String giving the prefix the master should use for assigning a unique job name.

       .. note:: Only characters in ``[a-zA-Z0-9_]`` are allowed in the prefix.

    .. attribute:: jobdict.scheduler

       Dictionary of options for the job scheduler.
       Currently supports the following keys:

                  * *max_cores* - use at most this many cores (applies to both map and reduce).
                    Default is ``2**31``.
                  * *force_local* - always run task on the node where input data is located;
                    never use HTTP to access data remotely.
                  * *force_remote* - never run task on the node where input data is located;
                    always use HTTP to access data remotely.

       .. versionadded:: 0.2.4

    .. attribute:: jobdict.owner

       String name of the owner of the :term:`job`.

.. _jobenvs:

Job Environment Variables
-------------------------

A :term:`JSON` dictionary of environment variables (with string keys
and string values).  The master will set these in the environment
before running the :attr:`jobdict.worker`.

.. _jobhome:

The Job Home
------------

The :term:`job home` directory serialized into :term:`ZIP` format.
The master will unzip this before running the :attr:`jobdict.worker`.
The :term:`worker` is run with this directory as its working
directory.

In addition to the :term:`worker` executable, the :term:`job home` can
be populated with files that are needed at runtime by the worker.
These could either be shared libraries, helper scripts, or parameter
data.

.. note:: The .disco subdirectory of the *job home* is reserved by Disco.

The job home is shared by all tasks of the same job on the same node.
That is, if the job requires two map task and two reduce task
executions on a particular node, then the job home will be unpacked
only once on that node, but the worker executable will be executed
four times in the job home directory, and it is also possible for some
of these executions to be concurrent.  Thus, the worker should take
care to use unique filenames as needed.

.. _jobdata:

Additional Job Data
-------------------

Arbitrary data included in the :term:`job pack`, used by the :term:`worker`.
A running worker can access the job pack at the path specified by ``jobfile``
in the response to the :ref:`TASK` message.

.. _submitting_jobpack:

Creating and submitting a Job Pack
----------------------------------

The jobpack can be constructed and submitted using the ``disco job``
command.
