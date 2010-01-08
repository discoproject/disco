
.. _overview:

Technical Overview
==================

.. image:: images/disco-arch.png

Disco is based on the master-slave architecture.

 * **Disco master** receives jobs from clients,
   adds them to the job queue, and runs them in the cluster when CPUs
   become available.

 * **Client processes** are Python programs that submit jobs to the
   master using the :func:`disco.job` function.

 * A **worker supervisor** is started automatically by the master on each node
   in the cluster. Its responsibility is to spawn and monitor all Python worker
   processes that run on that particular node.

 * **Python workers** take the user-specified :term:`job functions`, run them
   with the specified input, and write output to result files. As the result,
   they send addresses of the result files to the master through their
   supervisor.

 * Input files are accessed via HTTP, unless Python worker runs on the same node
   where an input file is located, in which case it will be read
   directly from the disk. To be able to access input files on remote
   nodes, a **httpd** daemon (web server), runs on each node. Disco
   master tries to maintain *data locality* by scheduling tasks on the
   same nodes where their inputs can be found.

Each map or reduce instance is given exclusive access to a CPU while
it executes. This means that in a cluster with *N* CPUs, at most *N*
Disco tasks can run in parallel.

If high availability of the system is a concern, CPUs in the cluster can
be partitioned amongst arbitrary many Disco masters. This way several
Disco masters can co-exist, which eliminates the only single point of
failure in the system.
