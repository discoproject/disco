
.. _tutorial:

Tutorial
========

This tutorial shows how to create and run a Disco job that counts words.
To start with, you need nothing but a single text file.
Let's call the file ``bigfile.txt``.
If you don't happen to have a suitable file on hand,
you can download one from `here <http://discoproject.org/media/text/bigfile.txt>`_.

1. Prepare input data
---------------------

Disco can distribute computation only as well as data can be distributed.
In general, we can push data to :ref:`DDFS`,
which will take care of distributing and replicating it.

.. note::
   Prior to Disco 0.3.2, this was done by splitting data manually,
   and then using ``ddfs push`` to push user-defined blobs.
   As of Disco 0.3.2, you can use ``ddfs chunk``
   to automatically chunk and push size-limited chunks to DDFS.
   See :ref:`chunking`.

Lets chunk and push the data to a tag ``data:bigtxt``::

      ddfs chunk data:bigtxt ./bigfile.txt

We should have seen some output telling us that the chunk(s) have been created.
We can also check where they are located::

    ddfs blobs data:bigtxt

and make sure they contain what you think they do::

    ddfs xcat data:bigtxt | less

.. note::
   Chunks are stored in Disco's internal compressed format,
   thus we use ``ddfs xcat`` instead of ``ddfs cat`` to view them.
   ``ddfs xcat`` applies some
   :func:`~disco.worker.classic.func.input_stream`\'s
   (by default, :func:`~disco.worker.classic.func.chain_reader`),
   whereas ``ddfs cat`` just dumps the raw bytes contained in the blobs.

If you used the file provided above,
you should have only ended up with a single chunk.
This is because the default chunk size is 64MB (compressed),
and the ``bigfile.txt`` is only 12MB (uncompressed).
You can try with a larger file to see that chunks are created as needed.

.. hint::
   If you have unchunked data stored in DDFS that you would like to chunk,
   you can run a Disco job, to parallelize the chunking operation.
   Disco includes an `example`_ of how to do this,
   which should work unmodified for most use cases.

.. _example: https://github.com/discoproject/disco/blob/master/examples/util/chunk.py

2. Write job functions
----------------------

Next we need to write :term:`map` and :term:`reduce` functions to count words.
Start your favorite text editor and create a file called ``count_words.py``.
First, let's write our map function::

        def fun_map(line, params):
                for word in line.split():
                        yield word, 1

Quite compact, eh?
The map function takes two parameters, here they are called *line* and *params*.
The first parameter contains an input entry, which is by default a line of text.
An input entry can be anything though,
since you can define a custom function that parses an input stream
(see the parameter *map_reader* in the
:func:`Classic Worker <disco.worker.classic.worker.Worker>`).
The second parameter, *params*, can be any object that you specify,
in case that you need some additional input for your functions.

For our example, we can happily process input line by line.
The map function needs to return an iterator over of key-value pairs.
Here we split a line into tokens using the builtin :meth:`string.split`.
Each token is output separately as a key, together with the value *1*.

Now, let's write the corresponding reduce function::

        def fun_reduce(iter, params):
                from disco.util import kvgroup
                for word, counts in kvgroup(sorted(iter)):
                        yield word, sum(counts)

The first parameter, *iter*,
is an iterator over those keys and values produced by the map function,
which belong to this reduce instance (see :term:`partitioning`).

In this case, words are randomly assigned to different reduce instances.
Again, this is something that can be changed
(see :func:`~disco.worker.classic.func.partition` for more information).
However, as long as all occurrences of the same word go to the same reduce,
we can be sure that the final counts are correct.

The second parameter *params* is the same as in the map function.

We simply use :func:`disco.util.kvgroup` to pull out each word along with its counts,
and sum the counts together, yielding the result.
That's it.
Now we have written map and reduce functions for counting words in parallel.

3. Run the job
--------------

Now the only thing missing is a command for running the job.
There's a large number of parameters that you can use to specify your job,
but only three of them are required for a simple job like ours.

In addition to starting the job, we want to print out the results as well.
First, however, we have to wait until the job has finished.
This is done with the :meth:`~disco.core.Disco.wait` call,
which returns results of the job once has it has finished.
For convenience, the :meth:`~disco.core.Disco.wait` method,
as well as other methods related to a job,
can be called through the :class:`~disco.job.Job` object.

A function called :func:`~disco.core.result_iterator` takes
a list of addresses to the result files, that is returned by
:meth:`~disco.core.Disco.wait`,
and iterates through all key-value pairs in the results.

The following example from ``examples/util/count_words.py`` runs the job,
and prints out the results:

    .. literalinclude:: ../../examples/util/count_words.py

.. note:: This example could also be written by extending :class:`disco.job.Job`.
          See, for example, `examples/util/wordcount.py`.

Now comes the moment of truth.

Run the script as follows::

        python count_words.py

If everything goes well, you will see that the job executes.
The inputs are read from the tag ``data:bigtxt``, which was created earlier.
Finally the output is printed.
While the job is running, you can point your web
browser at ``http://localhost:8989`` (or some other port where you run the
Disco master) which lets you follow the progress of your job in real-time.

You can also set :envvar:`DISCO_EVENTS` to see job events from your console::

       DISCO_EVENTS=1 python count_words.py

In this case, the events were anyway printed to the console,
since we specified ``show=True``.

What next?
----------

As you saw, creating a new Disco job is pretty straightforward.
You could extend this simple example in any number of ways.
For instance, by using the params object to include a list of stop words.

You could continue on with :ref:`tutorial_2` which is intended as a
follow-on tutorial to this one.

If you pushed the data to :ref:`DDFS`,
you could try changing the input to ``tag://data:bigtxt``,
and add ``map_reader = disco.worker.classic.func.chain_reader``.

You could follow the :ref:`discodb_tutorial`,
to learn more about using :mod:`discodb` with Disco.

You could try using :func:`~disco.worker.classic.func.sum_combiner`,
to make the job more efficient.

You can also experiment with custom partitioning and reader functions.
They are written in the same way as map and reduce functions.
Just see some examples in the :mod:`disco.worker.classic.func` module.
After that, you could try :ref:`chaining jobs together <chaining>`,
so that output of the previous job becomes input for the next one.

The best way to learn is to pick a problem or algorithm that you know
well, and implement it with Disco. After all, Disco was designed to
be as simple as possible so you can concentrate on your own problems,
not on the framework.
