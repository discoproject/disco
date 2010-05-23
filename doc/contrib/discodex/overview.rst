
Overview
========

.. contents::

Introduction
------------

A crucial step in building data-driven systems, is indexing the data in such a way that it is accessible in constant time.
Such random access is essential for building real-time systems, but also valuable in optimizing many other applications which rely upon lookups into the data.
Discodex builds on top of Disco, abstracting away some of the most common operations for organizing piles of raw data into :term:`indices <index>`, and querying them.
Discodex adopts a similar strategy to Disco in achieving this goal: making the interface simple and intuitive, so that development time isn't an excuse for not building an index.

.. image:: images/index_piles_of_data.png

In other words, Discodex is a distributed `(key, values)` storage system.
Discodex makes it easy to build and query indices using real-world datasets, especially when such indices would be too large to fit on a single machine.

Architecture
------------

When you run Discodex, you are running an `HTTP`_ server which maps `ReSTful`_ URIs to Disco jobs.
Discodex stores keys and values in indices using :ref:`DDFS`.
The individual files which are distributed, are called `index chunks` or :term:`ichunks <ichunk>`.
Discodex uses a custom data structure/file format for storing `ichunks`, called a :mod:`discodb`.

The overall architecture looks like this:

.. image:: images/discodex_design.png

.. _HTTP: http://www.w3.org/Protocols
.. _ReSTful: http://en.wikipedia.org/wiki/Representational_State_Transfer
