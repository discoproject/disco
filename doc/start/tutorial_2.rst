
.. _tutorial_2:

Tutorial 2
==========

This tutorial expands on the introductory :ref:`tutorial` to expose the
user to Disco's exported classes while solidifying the concepts of feeding
input into and capturing output from Disco jobs.  As a working example,
this tutorial walks the user through implementing one approach for
performing an `inner_join`_ operation on arbitrarily large datasets.

As a prerequisite, the reader is expected to
have successfully completed the introductory :ref:`tutorial` on a
functional (happily configured and working) installation of Disco.

.. _inner_join: http://en.wikipedia.org/wiki/Join_%28SQL%29#Inner_join

1. Background and sample input
------------------------------

Let's first prepare a sample input data set that's small enough and simple
enough for us to follow and know what to expect on output.  We will prepare
two sets of input in csv format to be "joined" together using the first
entry in each row as the key to match (join) on.  Create a file named
``set_A.csv`` containing the following text::

   1,"alpha"
   2,"beta"
   3,"gamma"
   4,"delta"
   5,"epsilon"

Create a second file named ``set_B.csv`` containing the following text::

   1,"who"
   2,"what"
   3,"where"
   4,"when"
   5,"why"
   6,"how"

When we `inner_join`_ these two datasets using the first entry in each row as
its key, we would like to see output that looks something like this::

    1,"alpha","who"
    2,"beta","what"
    3,"gamma","where"
    4,"delta","when"
    5,"epsilon","why"

Note that there is no line in the output for key=6 as seen in the input data
of ``set_B.csv`` because it did not have a matched pair for that key in
``set_A.csv``.  Please also note that we would expect the output to be the
same even if the order of the lines were scrambled in either of the two
input data sets.

.. note::
If you're a big data fanatic and can't wait to get to a macho volume of
input, *be patient*.  Let's make sure we get everything working right and we
understand what's happening with small data first before turning up the
volume.

You should now have two files in your working directory named ``set_A.csv``
and ``set_B.csv`` which contain 5 and 6 lines, respectively, of text data.

2. Split input data into chunks
-------------------------------

In the introductory :ref:`tutorial`, we made use of a ddfs (:ref:`DDFS`)
command, ``ddfs chunk``, to split input data into chunks and copy it onto
ddfs.  To provide a more concrete sense of how to chunk input data, let's
instead split our input data *before* we push it to ddfs.  When we do push
our already-split data to ddfs, we will tell ddfs to treat the distinct
chunks as one.

As alluded to before, there are many strategies for performing efficient
join operations inside MapReduce frameworks.  Here we will take the approach
of combining our two input data sets (A and B) into a single input stream.
With a single input stream, it's easier to see how to split up the input,
do work on it, then merge it back together.  This approach doesn't
necessarily harm performance but there are different strategies tuned for
optimal performance depending upon the nature of your data.  (Search the
net for "``mapreduce join``" to see the wealth of competing strategies out
there.)

Assuming a unix-like environment from here on, start by combining our two
input files::

   % cat set_A.csv set_B.csv > both_sets.csv

Next, we want to split our ``both_sets.csv`` file into chunks with 2 lines
each.  You can do this with a text editor yourself, by hand, or we can
make use of the convenient unix utility ``split`` to do the job for us::

   % split -l 2 both_sets.csv

Running ``split`` as above should create 6 files named ``xaa`` through
``xaf``.  You can quickly verify this by performing a count of the lines
in each file and seeing that it adds up to 11::

   % wc -l xa?
     2 xaa
     2 xab
     2 xac
     2 xad
     2 xae
     1 xaf
    11 total

Now that we've split the input data ourselves into 6 chunks, let's push
our split data into ddfs and label it all with a single tag,
``data:both_sets``, so that we can refer to all our chunks as one::

   % ddfs push data:both_sets ./xa?

You can verify that all 11 lines made it into ddfs and are accessible via
that single tag by asking to ``cat`` it back to the screen::

   % ddfs cat data:both_sets

By splitting our input data into 6 chunks, we are now set up to perform
6 executions of our :term:`map` function (which we have yet to implement).  If
you have a processor with 6 cores, you could conceivably perform all 6
map operations in parallel at the same time.  If you have more than 6 cores
either on one processor or across multiple processors available to Disco,
you'll only be able to make use of, at most, 6 of them at one time during
the map phase of a MapReduce job.  In general:  If you want more map
operations to be running at the same time, make more chunks (smaller chunks).
Taking it too far, if you make more chunks than you have cores, you won't
get further speedup from parallelism.

You should now have the 11 lines of input csv-format data stored in ddfs
in 6 chunks under the tag ``data:both_sets``.  While not necessarily the
best approach for splitting and importing your largest datasets into ddfs,
it may prove helpful to remember that you can chunk your data all at once
*or* bring it in in pieces.

3. Write a job using a derived class
------------------------------------

In the introductory :ref:`tutorial`, we defined a :term:`map` function and a
:term:`reduce` function then supplied them as parameters to ``Job().run()``.
But there's more fun to be had by deriving a new class from
:class:`~disco.job.Job`.  Let's start by declaring our new class and saving
it in a source file named ``simple_innerjoin.py``::

        class CsvInnerJoiner(Job):
            def map(self, row, params):
                # TODO
                pass
        
            def reduce(self, rows_iter, out, params):
                # TODO
                pass

Before we turn attention to implementing either of the :term:`map` or
:term:`reduce` methods, we should consider our need, in this example, to
read input that's in csv format.  A convenient solution is to implement
``map_reader()`` in our class::

            @staticmethod
            def map_reader(fd, size, url, params):
                reader = csv.reader(fd, delimiter=',')
                for row in reader:
                    yield row

This will allow us to implement ``map()`` to operate on one row's worth
of input data at a time without needing to worry about raw input format.

Our strategy with our :term:`map` and :term:`reduce` methods will be to
first sort all of the input data by their unique keys (which will put
row 4 from ``set_A.csv`` right next to / in front of row 4 from
``set_B.csv``), then merge consecutive rows having the same unique key.
This puts most of the burden on our ``reduce()`` implementation, but
we'll ease that a bit in a later pass.  Since ``map()`` does not need
to do much other than serve as a pass-through (quickly), modify our
placeholder for ``map()`` to read::

            def map(self, row, params):
                yield row[0], row[1:]

This will separate the unique key (in position 0) from all the other
data on a row (assuming we want to re-use this for something more
interesting than our fairly trivial input data set so far).

Now we ask ``reduce()`` to do the real work in its updated definition::

            def reduce(self, rows_iter, out, params):
                from disco.util import kvgroup
                from itertools import chain
                for url_key, descriptors in kvgroup(sorted(rows_iter)):
                    merged_descriptors = list(chain.from_iterable(descriptors))
                    if len(merged_descriptors) > 1:
                        out.add(url_key, merged_descriptors)

Again, as in :ref:`tutorial`, we are using :func:`disco.util.kvgroup`
to group together consecutive rows in our sorted input and hand them
back as a group (iterable).  Note our test to see if we have a matched pair
or not is somewhat fragile and may not work for more general cases -- we
highlight this as an area for improvement for the reader to consider
later.

Let's round out our ``simple_innerjoin.py`` tool by making it easy to
supply names for input and output, while also making our output come out
in csv format -- adding to the bottom of ``simple_innerjoin.py``::

        if __name__ == '__main__':
            input_filename = "input.csv"
            output_filename = "output.csv"
            if len(sys.argv) > 1:
                input_filename = sys.argv[1]
                if len(sys.argv) > 2:
                    output_filename = sys.argv[2]
        
            from simple_innerjoin import CsvInnerJoiner
            job = CsvInnerJoiner().run(input=[input_filename])
        
            with open(output_filename, 'w') as fp:
                writer = csv.writer(fp)
                for url_key, descriptors in result_iterator(job.wait(show=True)):
                    writer.writerow([url_key] + descriptors)

.. note::
   Notice the important nuance in our importing the ``CsvInnerJoiner`` class
   from our own source file.  Ordinarily, if this script were run
   independently, we would not expect to need to import a class that's being
   defined in the same source file.  Because Disco `pickle`_'s this source file
   (using its own :class:`dPickle`) for the sake of distributing it to worker
   nodes, upon unpickling the definition of ``CsvInnerJoiner`` will no longer
   be visible in the local context.  Try running with the "from ..." line
   commented out to see the resulting complaint from the Unpickler run by
   the workers.  If anything, we should take this as a gentle reminder to be
   cognizant that we are preparing code to run in a distributed, parallel
   system and that we occasionally need to make some small adjustments for
   that environment.

.. _pickle: http://docs.python.org/library/pickle.html

In the prior :ref:`tutorial`, all output flowed to the screen (stdout) but
here we capture the output flowing from our job into a file in csv format.
We chose to use the csv format throughout this :ref:`tutorial_2` for
convenience but clearly other methods of redirecting output and formatting
it to your own needs are possible in the same way.

4. Results and exploring partitions
-----------------------------------

We should now be set up to run our job with 6 input chunks corresponding
to 6 invocations of our ``map()`` method and the output of those map runs
will flow into 1 invocation of our ``reduce()`` method to then produce our
final csv result file.  Launching from the command-line::

    % python simple_innerjoin.py data:both_sets output.csv

At this point, please check that the output found in the file ``output.csv``
matches what was expected.  (Pedants can play further with formatting and
quotation rules via the csv module, to taste.)  If you instead encounter
errors, please double-check that your file faithfully matches the code
outlined thus far and please double-check that you can still run the
example from the introductory :ref:`tutorial`.

Thus far we've been running parallel invocations of ``map()`` but not of
``reduce()`` -- let's change that by requesting that the output from the
map phase be divided into 2 partitions.  Add the following line to the 
very top of our definition of the ``CsvInnerJoiner`` class, to look
something like this::

        class CsvInnerJoiner(Job):
            partitions = 2
            
            ...*truncated*...

Run the job again from the command-line and this time you may find that
while the output might be correct, the output is no longer in sort-order.
This is because we did not sort over all rows -- only the rows handed to a
particular invocation of ``reduce()`` were sorted, though we still get to
see the output from parallel invocations of ``reduce()`` concatenated
together in our single output csv file.

This helps highlight a problem we're going to have once we start throwing
larger volumes of data at this Disco job:  invoking ``sorted()`` requires
a potentially large amount of memory.  Thankfully Disco provides, as part
of its framework, an easier solution to this common need for working with
sorted results in the reduce step.  At the top of our definition of the
``CsvInnerJoiner`` class, let's add the following line::

        class CsvInnerJoiner(Job):
            partitions = 2
            sort = True
            
            ...*truncated*...

Simultaneously, we can remove the use of ``sorted()`` from the one line
in our implementation of ``reduce()`` so that it now reads as::

            def reduce(self, rows_iter, out, params):
                from disco.util import kvgroup
                from itertools import chain
                for url_key, descriptors in kvgroup(rows_iter):
                    merged_descriptors = list(chain.from_iterable(descriptors))
                    if len(merged_descriptors) > 1:
                        out.add(url_key, merged_descriptors)

Now the work of sorting the results flowing from the mappers is done for
us by the framework and that sort is performed across all mappers' results
before being partitioned and handed as input to the reducers.

5. Big(ger) Data
----------------

Let's quickly generate a bigger input data set with which to work.  The
following one-liner can be modified to generate as little or as much sample
data as you have patience / disk space to hold -- modify the ``1000000`` near
the end of the line to create as many rows of data as you like::

    % python -c "import csv, sys, random; w = csv.writer(sys.stdout); 
    [w.writerow([i, int(999999*random.random())]) for i in range(1000000)]" > input1.csv

Run it twice (saving the first run's output in a different name from the
second run's) to give yourself two sets of input data just as before. 
Then follow the steps from either this :ref:`tutorial_2` or the prior
introductory :ref:`tutorial` to chunk the input data and push it to ddfs
in whatever manner you like.  (Let's assume you tag your chunked input
data as ``data:bigger_sets`` in ddfs.)

The only modification to ``simple_innerjoin.py`` that we suggest,
depending upon how large your newly generated input data set is, is to
increase the number of partitions to ratchet up the number of parallel
runs of ``reduce()``.  Then go ahead and run your job in the same way::

    % python simple_innerjoin.py data:bigger_sets bigger_output.csv

By monitoring the processes on the system(s) where you've configured
Disco, you will hopefully be able to observe individual workers performing
their map tasks and reduce tasks, the framework doing your sorting work
for you in between, and how much cpu processing time is being used versus
time spent waiting on disk or other resources.  Having a larger dataset
with a longer runtime makes observing these things much easier.

Note that you may quickly find your disk access speed to become a
bottleneck and for this reason and others you should consider playing with
the number of partitions as well as the number of input chunks (how many
reducers and mappers, respectively) to find your system's optimal
throughput for this job.

As a variation on the above, remember that our ``simple_innerjoin.py``
script has the capability to read its input data from a local file instead
of ddfs -- try running again with a local file supplied as the location of
the input (instead of ``data:bigger_sets``).  Did you get an error message
with "Invalid tag (403)"?  If so, you need to ensure Disco recognizes that
you are supplying a filename and not the name of a tag.  Did you get an
error message with "IOError: [Errno 2] No such file or directory"?  If so,
you either need to supply the full path to the file (not a relative path
name) or that path may not be available to Disco everywhere (if so, a good
reason to use ddfs again).  Was your run faster or slower than using ddfs?

After playing with ever larger volumes of data and tweaking the controls
that Disco provides, you'll quickly gain confidence in being able to throw
any size job at Disco and knowing how to go about implementing a solution.

simple_innerjoin.py listing
---------------------------

Complete source all in one place:

    .. literalinclude:: ../../examples/util/simple_innerjoin.py

What next?
----------

A natural next step in experimenting with partitioning involves
:ref:`chaining jobs together <chaining>` since the number of partitioned
outputs from one job becomes the number of chunked inputs for the next.
As a baby step, you could move the ``reduce()`` method implemented above
into a second, chained job and replace it in the first job with a 
do-nothing substitute like :func:`disco.worker.classic.func.nop_reduce`.

As already mentioned in the introductory :ref:`tutorial`,
the best way to learn is to pick a problem or algorithm that you know
well, and implement it with Disco. After all, Disco was designed to
be as simple as possible so you can concentrate on your own problems,
not on the framework.
