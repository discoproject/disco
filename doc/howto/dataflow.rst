.. _dataflow:

Data Flow in Disco Jobs
=======================

Disco allows the chaining together of jobs containing
:func:`disco.func.map` and/or :func:`disco.func.reduce` tasks.
`Map` and `Reduce` phases each have their own concepts of data flow.
Through combinations of chained jobs, Disco supports a remarkable
variety of data flows.

Understanding data flow in Disco requires understanding the core concept of :term:`partitioning`.
Map results in Disco can be either `partitioned` or `non-partitioned`.
For partitioned output, the results Disco returns are index files,
which contain the URLs of each individual output partition:

.. figure:: ../images/dataflow/partitioned_file.png

   *Partitioned File Structure*

In the diagrams below, it should be clear when Disco is relying on
either reading partitioned input or writing partitioned output.

Map Flows
---------

The two basic modes of operation for the map phase correspond directly
to writing either partitioned or non-partitioned output.

For non-partitioned map, *every* map task creates a single output.
In other words, the input-output relationship is exactly `1 to 1`:

.. _non-partitioned_map_flow:

.. figure:: ../images/dataflow/non-partitioned_map_flow.png

   *Non-Partitioned Map*


For partitioned map, the :func:`disco.func.partition` function is called
for every ``(key, value)`` pair emitted by :func:`disco.func.map`.
The partition function determines which partition the pair will be written to:

.. _partitioned_map_flow:

.. figure:: ../images/dataflow/partitioned_map_flow.png

   *Partitioned Map*


Notice that for partitioned output with ``N`` partitions, **exactly** ``N``
files will be created *for each node*, *regardless of the number of maps*.
If the map tasks run on ``K`` nodes, exactly ``K * N`` files will be created.
Whereas for non-partitioned output with ``M`` inputs,
exactly ``M`` output files will be created.

This is an important difference between partitioned output with 1 partition,
and non-partitioned output:

.. _one_partition_map_flow:

.. figure:: ../images/dataflow/one_partition_map_flow.png

   *Single-Partition Map*


The default number of partitions for map is 1.
This means that by default if you run ``M`` maps on ``K`` nodes,
you end up with ``K`` files containing the results.
In older versions of Disco, there were no partitions by default,
so that jobs with a huge number of inputs produced a huge number of outputs.
If ``M >> K``, this is suboptimal for the reduce phase.


Reduce Flows
------------

The basic modes of operation for the reduce phase correspond to
partitioned/non-partitioned input (instead of output as in the map phase).

For non-partitioned input, there can only ever be 1 reduce task:

.. _non-partitioned_reduce_flow:

.. figure:: ../images/dataflow/non-partitioned_reduce_flow.png

   *Non-Partitioned Reduce Flow*


The situation is slightly more complicated for partitioned input,
as there is a choice to be made whether or not to merge the partitions,
so that all results are handled by a single reduce:

.. _merge_partitioned_reduce_flow:

.. figure:: ../images/dataflow/merge_partitioned_reduce_flow.png

   *Merge Partitioned Reduce Flow*


Or to use the normal, distributed reduce,
in which there are ``N`` reduces for ``N`` partitions:

.. _normal_partitioned_reduce_flow:

.. figure:: ../images/dataflow/normal_partitioned_reduce_flow.png

   *Normal Partitioned Reduce Flow*


As you might expect, the default is to distribute the reduce phase.
