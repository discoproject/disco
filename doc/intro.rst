
.. _disco:

What is Disco?
==============

Disco is an implementation of :term:`mapreduce` for distributed computing.
Disco supports parallel computations over large data sets,
stored on an unreliable cluster of computers,
as in the original framework created by Google.
This makes it a perfect tool for analyzing and processing large data sets,
without having to worry about difficult technicalities related to distribution
such as communication protocols, load balancing,
locking, job scheduling, and fault tolerance,
which are handled by Disco.

Disco can be used for a variety data mining tasks:
large-scale analytics,
building probabilistic models, and
full-text indexing the Web,
just to name a few examples.

Batteries included
------------------

The Disco core is written in `Erlang <http://www.erlang.org>`_,
a functional language that is designed for building robust fault-tolerant
distributed applications.
Users of Disco typically write jobs in Python,
which makes it possible to express even complex algorithms with very little code.

For instance, the following fully working example computes word
frequencies in a large text:

.. literalinclude:: ../examples/util/count_words.py

Disco is designed to integrate easily in larger applications, such as
Web services, so that computationally demanding tasks can be delegated
to a cluster independently from the core application. Disco provides an
extremely compact Python API -- typically only two functions are needed --
as well as a REST-style Web API for job control and a easy-to-use Web
interface for status monitoring.

Disco also exposes a simple worker protocol, allowing jobs to be
written in any language that implements the protocol.

Distributed computing made easy
-------------------------------

Disco is a good match for a cluster of commodity Linux servers. New
nodes can be added to the system on the fly, by a single click on
the Web interface. If a server crashes, active jobs are automatically
re-routed to other servers without any interruptions. Together with
an automatic provisioning mechanism, such as
`Fully Automatic Installation <http://www.informatik.uni-koeln.de/fai/>`_,
even a large cluster can be maintained with only a minimal amount
of manual work. As a proof of concept,
`Nokia Research Center in Palo Alto <http://research.nokia.com>`_
maintains an 800-core cluster running Disco using this setup.


Main features
-------------

- Proven to scale to hundreds of CPUs and tens of thousands of simultaneous
  tasks.

- Used to process datasets in the scale of tens of terabytes.

- Extremely simple to use: A typical tasks consists of two functions written
  in Python and two calls to the Disco API.

- Tasks can be specified in any other language as well, by
  implementing the Disco worker protocol.

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

- Comes with a built-in distributed storage system (:ref:`ddfs`).
