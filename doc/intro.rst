
.. _disco:

What is Disco?
==============

Disco is an implementation of the `Map-Reduce framework
<http://en.wikipedia.org/wiki/MapReduce>`_ for distributed computing. As
the original framework, which was publicized by Google, Disco supports
parallel computations over large data sets on unreliable cluster of
computers. This makes it a perfect tool for analyzing and processing large
datasets without having to bother about difficult technical questions
related to distributed computing, such as communication protocols, load
balancing, locking, job scheduling or fault tolerance, which are taken
care by Disco.

Disco, standing on the shoulders of the solid Map-Reduce model, is
suitable and already used for various data mining tasks, large-scale
Web analytics, and building probabilistic models and full-text indices,
to name a few examples.

Batteries included
------------------

In contrast to a well-known open-source implementation of the Map-Reduce
framework, `Hadoop <http://hadoop.apache.org>`_, that is implemented in
Java, the Disco core is written in `Erlang <http://www.erlang.org>`_,
a functional language that is designed for building robust fault-tolerant
distributed applications. Users of Disco typically write jobs in Python,
which makes it possible to express even complex algorithms or data
processing tasks often only in tens of lines of code.

For instance, the following fully working example computes word
frequencies in a large text corpus using 100 CPUs in parallel:

::

    from disco.core import Disco, result_iterator

    def fun_map(e, params):
        return [(w, 1) for w in re.sub("\W", " ", e).lower().split()]

    def fun_reduce(iter, out, params):
        s = {}
        for k, v in iter:
            if k in s:
                s[k] += int(v)
            else:
                s[k] = int(v)
        for k, v in s.iteritems():
            out.add(k, v)

    results = Disco("disco://localhost").new_job(
                name = 'wordcount',
                map = fun_map,
                reduce = fun_reduce,
                input = ['http://localhost/text-block-1', 'http://localhost/text-block-2'],
                partitions = 100).wait()

    for key, value in result_iterator(results):
	    print key, value

Disco is designed to integrate easily in larger applications, such as
Web services, so that computationally demanding tasks can be delegated
to a cluster independently from the core application. Disco provides an
extremely compact Python API -- typically only two functions are needed --
as well as a REST-style Web API for job control and a easy-to-use Web
interface for status monitoring.

High-performance computing made easy
------------------------------------

Disco is a good match for a cluster of commodity Linux servers. New
nodes can be added to the system on the fly, by a single click on
the Web interface. If a server crashes, active jobs are automatically
re-routed to other servers without any interruptions. Together with
an automatic provisioning mechanism, such as the `Fully Automatic
Installation for Debian <http://www.informatik.uni-koeln.de/fai/>`_,
even a large HPC cluster can be maintained with only a minimal amount
of manual work. As a proof of concept, `Nokia Research Center in Palo
Alto <http://research.nokia.com>`_ maintains a 800-core cluster running
Disco using this setup.


Main features
-------------

- Proven to scale to hundreds of CPUs and tens of thousands of simulataneous
  tasks.

- Used to process datasets in the scale of tens of terabytes.

- Extremely simple to use: A typical tasks consists of two functions written
  in Python and two calls to the Disco API.

- Tasks can be specified in any other language as well, via a simple external
  interface based on standard input and output streams. Bindings for C are
  provided as an example.

- Input data can be in any format, even binary data such as images. The
  data can be located on any source that is accesible by HTTP or it can
  distributed to local disks.

- Fault-tolerant: Server crashes don't interrupt jobs. New servers can be
  added to the system on the fly.

- Flexible: In addition to the core map and reduce functions, a combiner
  function, a partition function and an input reader can be provided by
  the user.

- Easy to integrate to larger applications using the standard Disco module
  and the Web APIs.

- Comes with a built-in distributed storage system (:ref:`ddfs`) and
  a distributed indexing subsystem (:ref:`discodex`), enabling ad-hoc
  querying of terabytes of data.
