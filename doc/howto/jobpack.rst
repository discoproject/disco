.. _jobpack:

The :term:`Job Pack <job pack>`
===============================

The *job pack* contains all the information needed for creating a running a Disco :term:`job`.

File format::

        +---------------- 4
        | magic / version |
        +---------------- 8 -------------- 12 ------------- 16 ------------- 20
        | jobdict offset  | jobenvs offset | jobhome offset | jobdata offset |
        +--------------------------------------------------------------------+
        |                           ... reserved ...                         |
        128 -----------------------------------------------------------------+
        |                               jobdict                              |
        +--------------------------------------------------------------------+
        |                               jobenvs                              |
        +--------------------------------------------------------------------+
        |                               jobhome                              |
        +--------------------------------------------------------------------+
        |                               jobdata                              |
        +--------------------------------------------------------------------+


.. _jobdict:

The :term:`Job Dict <job dict>`
-------------------------------

fields in the job dict

.. _jobenvs:

Job Environment Variables
-------------------------

env vars

.. _jobhome:

The :term:`Job Home <job home>`
-------------------------------

the job home

.. _jobdata:

Additional Job Data
-------------------

XXX
