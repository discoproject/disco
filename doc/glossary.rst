
Glossary
========

.. glossary::

   conjunctive normal form
        See `conjunctive normal form <http://en.wikipedia.org/wiki/Conjunctive_normal_form>`_.

   blob
        An arbitrary file stored in :ref:`DDFS`.

        See also :ref:`blobs`.

   DDFS
        See :ref:`DDFS`.

   ichunk
        An :term:`immutable` piece of a distributed :term:`index`, stored in a file.

   immutable
        See `immutable object <http://en.wikipedia.org/wiki/Immutable_object>`_.

   index
        A mapping from each key in a set of keys to a multiset of values.
        Indices provide random access into a set of data.
        As an example, search engines are usually implemented using a
        `web index <http://en.wikipedia.org/wiki/Index_(search_engine)>`_.

   map
        The first phase of a :term:`job`,
        in which :term:`tasks <task>` are usually scheduled on the same node where their input data is hosted,
        so that local computation can be performed.

        Also refers to an individual task in this phase,
        which produces records that may be :term:`partitioned <partitioning>`,
        and :term:`reduced <reduce>`.
        Generally there is one map task per input.

   master
        Distributed core that takes care of managing :term:`jobs <job>`,
        garbage collection for :term:`DDFS`, and other central processes.

        See also :ref:`overview`.

   job
        A set of map and/or reduce :term:`tasks <task>`,
        coordinated by the Disco :term:`master`.
        When the master receives a :class:`disco.job.JobPack`,
        it assigns a unique name for the job,
        and assigns the tasks to :term:`workers <worker>`
        until they are all completed.

        See also :mod:`disco.job`

   job functions
        Job functions are the functions that the user can specify for a
        :mod:`disco.worker.classic.worker`.
        For example,
        :func:`disco.worker.classic.func.map`,
        :func:`disco.worker.classic.func.reduce`,
        :func:`disco.worker.classic.func.combiner`, and
        :func:`disco.worker.classic.func.partition` are job functions.

   job dict
       The first field in a :term:`job pack`,
       which contains parameters needed by the master for job execution.

       See also :ref:`jobdict` and :attr:`disco.job.JobPack.jobdict`.

   job home
        The working directory in which a :term:`worker` is executed.
        The :term:`master` creates the *job home* from a :term:`job pack`,
        by unzipping the contents of its :ref:`jobhome <jobhome>` field.

        See also :ref:`jobhome` and :attr:`disco.job.JobPack.jobhome`.

   job pack
        The packed contents sent to the master when submitting a new job.
        Includes the :term:`job dict` and :term:`job home`, among other things.

        See also :ref:`jobpack` and :class:`disco.job.JobPack`.

   mapreduce
        A paradigm and associated framework for distributed computing,
        which decouples application code from the core challenges of
        fault tolerance and data locality.
        The framework handles these issues so that :term:`jobs <job>`
        can focus on what is specific to their application.

        See `MapReduce <http://en.wikipedia.org/wiki/MapReduce>`_.

   partitioning
        The process of dividing output records into a set of
        labelled bins, much like :term:`tags <tag>` in :term:`DDFS`.
        Typically, the output of :term:`map` is partitioned,
        and each :term:`reduce` operates on a single partition.

   persistent
        See `persistent data structure <http://en.wikipedia.org/wiki/Persistent_data_structure>`_.

   reduce
        The last phase of a :term:`job`,
        in which non-local computation is usually performed.

        Also refers to an individual :term:`task` in this phase,
        which usually has access to all values for a given key
        produced by the :term:`map` phase.
        Grouping data for reduce is achieved via :term:`partitioning`.

   tag
        A labelled collection of data in :term:`DDFS`.

        See also :ref:`tags`.

   task
        A *task* is essentially a unit of work, provided to a :term:`worker`.
        A Disco :term:`job` is made of :term:`map` and :term:`reduce` tasks.

        See also :mod:`disco.task`.

   worker
        A *worker* is responsible for carrying out a :term:`task`.
        A Disco :term:`job` specifies the executable that is the worker.
        Workers are scheduled to run on the nodes,
        close to the data they are supposed to be processing.

        See also :mod:`disco.worker`.
