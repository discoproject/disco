
.. _discodb_tutorial:

================
DiscoDB Tutorial
================

This tutorial is a guide to using :class:`DiscoDBs <discodb.DiscoDB>` in Disco.

.. seealso:: :mod:`discodb`.

Create a DiscoDB
================

First, let's modify the :ref:`word count example <tutorial>` to write its output to a
:class:`DiscoDB <discodb.DiscoDB>`:

.. literalinclude:: ../../examples/util/wordcount_ddb.py

Notice how all we needed to do was change the
*reduce_output_stream* to :func:`disco.worker.classic.func.discodb_stream`,
and turn the count into a :class:`str`.
Remember, DiscoDBs only store byte sequences as keys and values,
its up to the user to serialize objects; in this case we just use str.

Query a DiscoDB
===============

Next, lets write a job to query the :class:`DiscoDBs <discodb.DiscoDB>`.

.. literalinclude:: ../../examples/util/query_ddb.py

Now let's try creating our word count db from scratch, and querying it::

        $ cd disco/examples/util
        $ disco run wordcount_ddb.WordCount http://discoproject.org/media/text/chekhov.txt
        WordCount@515:66e88:d39
        $ disco results @ | xargs python query_ddb.py 'word'
        word    18
        $ disco results @?WordCount | xargs python query_ddb.py 'this | word'
        this | word     217

.. hint:: The special arguments ``@`` and ``@?<string>``
          are replaced by the most recent job name and
          the most recent job with name matching ``<string>``, respectively.
          See :mod:`discocli`.

There are a few things to note in this example.
First of all, we use a :func:`disco.worker.classic.func.nop_map`,
since we do all the real work in our *map_reader*.
We use a builtin :func:`disco.worker.classic.input_stream`,
to return a :class:`DiscoDB <discodb.DiscoDB>` from the file on disk,
and that's the object our *map_reader* gets as a handle.

Notice also how we turn *vs* into a list.
This is because :meth:`discodb.DiscoDB.metaquery` returns lazy objects,
which cannot be pickled.

Finally, notice how we run the :class:`disco.job.Job`.
We make the *input* and *params* runtime parameters,
since we are pulling them from the command line.
When we iterate over the results,
we deserialize our counts using :class:`int`,
and sum them together.

Make sure you understand why we sum the counts together,
why there could possibly be more than one count in the result
(even though we only had one global :term:`reduce` in our word count job).

.. hint:: Look at the second query we executed on the command line.
