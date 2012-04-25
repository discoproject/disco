
Glossary
========

.. glossary::

   client
        The program which submits a :term:`job` to the :term:`master`.

   blob
        An arbitrary file stored in :ref:`DDFS`.

        See also :ref:`blobs`.

   data locality
        Performing computation over a set of data near where the data is located.
        Disco preserves *data locality* whenever possible,
        since transferring data over a network can be prohibitively expensive
        when operating on massive amounts of data.

        See `locality of reference <http://en.wikipedia.org/wiki/Locality_of_reference>`_.

   DDFS
        See :ref:`DDFS`.

   Erlang
        See `Erlang <http://en.wikipedia.org/wiki/Erlang_(programming_language)>`_.

   garbage collection (GC)
        DDFS has a tag-based filesystem, which means that a given blob
        could be addressed via multiple tags.  This means that blobs
        can only be deleted once the last reference to it is deleted.
        DDFS uses a garbage collection procedure to detect and delete
        such unreferenced data.

   immutable
        See `immutable object <http://en.wikipedia.org/wiki/Immutable_object>`_.

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

   JSON
        JavaScript Object Notation.

        See `Introducing JSON <http://www.json.org>`_.

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

   pid
        A process identifier.
        In Disco this usually refers to the :term:`worker` *pid*.

        See `process identifier <http://en.wikipedia.org/wiki/Process_identifier>`_.

   reduce
        The last phase of a :term:`job`,
        in which non-local computation is usually performed.

        Also refers to an individual :term:`task` in this phase,
        which usually has access to all values for a given key
        produced by the :term:`map` phase.
        Grouping data for reduce is achieved via :term:`partitioning`.

   replica
        Multiple copies (or replicas) of blobs are stored on different
        cluster nodes so that blobs are still available inspite of a
        small number of nodes going down.

   re-replication
        When a node goes down, the system tries to create additional
        replicas to replace copies that were lost at the loss of the
        node.

   SSH
        Network protocol used by :term:`Erlang` to start :term:`slaves <slave>`.

        See `SSH <http://en.wikipedia.org/wiki/Secure_Shell>`_.

   slave
        The process started by the :term:`Erlang` `slave module`_.

        .. _slave module: http://www.erlang.org/doc/man/slave.html

        See also :ref:`overview`.

   stdin
        The standard input file descriptor.
        The :term:`master` responds to the :term:`worker` over *stdin*.

        See `standard streams <http://en.wikipedia.org/wiki/Standard_streams>`_.

   stdout
        The standard output file descriptor.
        Initially redirected to :term:`stderr` for a Disco :term:`worker`.

        See `standard streams <http://en.wikipedia.org/wiki/Standard_streams>`_.

   stderr
        The standard error file descriptor.
        The :term:`worker` sends messages to the :term:`master` over *stderr*.

        See `standard streams <http://en.wikipedia.org/wiki/Standard_streams>`_.

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

        .. seealso::
           :mod:`The Python Worker module<disco.worker>`, and
           :ref:`worker_protocol`.

   ZIP
        Archive/compression format, used e.g. for the :term:`job home`.

        See `ZIP <http://en.wikipedia.org/wiki/ZIP_(file_format)>`_.
