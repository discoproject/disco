
.. _tutorial:

Tutorial
========

This tutorial shows how to create and run a Disco job that counts words in a
large text file. To start with, you need nothing but a single large text file.
Let's call the file ``bigfile.txt``.

Prepare input data
------------------

Disco can distribute computation only if data is distributed as well. Thus
our first step is to split ``bigfile.txt`` into small chunks. There is a
standard Unix command, ``split``, that can split a file into many pieces,
which is exactly what we want. We need also a directory where the chunks
are stored.  Let's call it ``bigtxt``::

        % mkdir bigtxt
        % split -l 100000 bigfile.txt bigtxt/bigtxt-

After running these lines, the directory ``bigtxt`` contains many files, named
like ``bigtxt-aa``, ``bigtxt-ab`` etc. which each contain 100,000 lines (except
the last chunk that might contain less).

If your ``bigfile.txt`` contains less than 100,000 lines, you can make the chunk
size smaller. The more chunks you have, the more processes you can run in
parallel. However, since launching a new process is not free, you shouldn't make
the chunks too small.

Distribute chunks to the cluster
--------------------------------

In theory you could use the chunks in the ``bigtxt`` directory
directly, but in practice it is a good idea to distribute the IO load
to many separate servers.  Disco provides a utility script called
``distrfiles.py`` that distributes files from a directory to the cluster.

Run it as follows::

        % python disco/util/distrfiles.py bigtxt /etc/nodes > bigtxt.chunks

Here ``bigtxt`` refers to the directory that contains the files and
``/etc/nodes`` is a file that lists available nodes in the cluster. The
script copies files to the nodes randomly, and outputs location of
each file to the standard output, which we capture here to the file
``bigtxt.chunks``. Take a look at that file to get an idea how inputs
are specified for Disco.

If you want to repeat this command multiple times, e.g. for a new set of
chunks, you need to run::

        % REMOVE_FIRST=1 python disco/util/distrfiles.py bigtxt /etc/nodes > bigtxt.chunks

which first removes the target directory on each node before copying
the chunks. 

Write job functions
-------------------

Next we need to write map and reduce functions to count the words in
the chunks.

Start your favorite text editor and open a file called, say,
``count_words.py``. Let's write first our map function::

        def fun_map(e, params):
                return [(w, 1) for w in e.split()]

Quite compact, eh? The map function always takes two parameters, here they
are called *e* and *params*. The first parameter contains an input entry,
which is by default a line of input. An input entry can be anything,
as you can define a custom function that extracts them from an input
stream --- see the parameter *map_reader* in :func:`disco.job` for more
information. The second parameter, *params*, can be any object that you
specify, in case that you need some additional input for your functions.

However, here we can happily process input line by line. The map function
needs to return a list of key-value pairs, specified as tuples. Here we split a
line into tokens with the standard ``string.split()`` function. Each token is
output separately as a key, together with the value *1* which says that we found
an occurrence of this word in the text. 

Now, let's write the corresponding reduce function::

        def fun_reduce(iter, out, params):
                stats = {}
                for word, count in iter:
                        if word in stats:
                                stats[word] += 1
                        else:
                                stats[word] = 1
                for word, total in stats.iteritems():
                        out.add(word, total)

The reduce function takes three parameters: The first parameter, *iter*,
is an iterator that loops through the intermediate values produced by
the map function, which belong to this reduce instance or partition.

In this case, different words are randomly assigned to different reduce
instances. Again, this is something that can be changed --- see the
parameter *partition* in :func:`disco.job` for more information. However,
as long as all occurrences of the same word go to the same reduce,
we can be sure that the final counts are correct.

So we iterate through all the words, and increment a counter in the
dictionary *stats* for each word. Once the iterator has finished, we know the
final counts, which are then sent to the output stream using the *out* object.
The object contains a method, *out.add(key, value)* that takes a key-value
pair and saves it to a result file.

The third parameter *params* contains the same additional input as in
the map function.

That's it. Now we have written map and reduce functions for counting
words in parallel!

Run job
-------

Now the only thing missing is a command for running the job. In Disco,
a single function call is needed to start a new job, which is aptly
called :func:`disco.job`. There's a large number of parameters that you can
use to tune your job but luckily only the first three of them are required.

In addition to starting the job, we want to print out the results as well.
A function called :func:`disco.result_iterator` takes a list of addresses to
the result files, that is returned by the :func:`disco.job` call, and iterates
through all key-value pairs in the results.

The following lines run the job and print out the results. Write them to the end
of your file::

        import disco, sys
        results = disco.job(sys.argv[1], "disco_tut", sys.argv[2:], fun_map, reduce = fun_reduce)
        for word, total in disco.result_iterator(results):
                print word, total

Here we read the address of the Disco master and the input files from the
command line. The map function is given as the third parameter, *fun_map*, and
the reduce function as the keyword parameter *reduce = fun_reduce* for
:func:`disco.job`.

Now comes the moment of truth. Run the script as follows::

        % export PYTHONPATH=disco/py
        % python count_words.py disco://localhost:5000 `cat bigtxt.chunks` > bigtxt.results

If everything goes well, the script pauses for some time while the
job executes. The inputs are read from the file ``bigtxt.chunks``
which was created earlier. Finally the outputs are written to
``bigtxt.results``.  While the job is running, you can point your web
browser at ``http://localhost:5000`` which lets you follow the progress
of your job in real-time.

Note that in your case the Disco master, specified here by
``disco://localhost:5000``, might be running on a different address. If you
can't find Disco at ``http://localhost:5000`` in your browser, consult
your nearest sysadmin for the correct settings.

Conclusion
----------

As you saw, creating a new Disco job is pretty straightforward. Next you could
write functions for a bit more complex job, which could, for instance, count
only words that are provided as a parameter to the map function.

It is highly recommended that you take a look in :mod:`homedisco`. It
is a simple replacement for :func:`disco.job` that lets you to debug,
profile and test your Disco functions on your local machine, instead of
running them in the cluster. It is an invaluable tool when developing
new programs for Disco.

You can also experiment with providing custom partitioning and reader
functions. They are written in the same way as map and reduce functions.
Just see some examples in the :mod:`disco` module. After that, you could
try to chain many map/reduce jobs together, so that outputs of the previous
job are used as the inputs for the next one --- in that case you need
to use :func:`disco.chain_reader`.

The best way to learn is to pick a problem or algorithm that you know
well, and implement it with Disco. After all, Disco was designed to
be as simple as possible so you can concentrate on your own problems,
not on the framework.
