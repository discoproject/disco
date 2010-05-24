
.. _tutorial:

Tutorial
========

This tutorial shows how to create and run a Disco job that counts
words in a large text file. To start with, you need nothing but a
single large text file.  Let's call the file ``bigfile.txt``. If
you don't happen to have a suitable file at hand, you can
download one from `http://discoproject.org/bigfile.txt
<http://discoproject.org/bigfile.txt>`_.

Some steps are executed slightly differently on a local cluster (or on
a single machine) and Amazon EC2. In these cases, you can find separate
instructions for the two environments. Follow the one that applies to
your case. Note that if you run the steps on the master node of your
EC2 cluster, in contrast to a remote machine that communicates with the
master node, you can follow the instructions for local clusters.


1. Prepare input data
---------------------

Disco can distribute computation only if data is distributed as well. Thus
our first step is to split ``bigfile.txt`` into small chunks. There is a
standard Unix command, ``split``, that can split a file into many pieces,
which is exactly what we want. We need also a directory where the chunks
are stored.  Let's call it ``bigtxt``::

        mkdir bigtxt
        split -l 100000 bigfile.txt bigtxt/bigtxt-

After running these lines, the directory ``bigtxt`` contains many files, named
like ``bigtxt-aa``, ``bigtxt-ab`` etc. which each contain 100,000 lines (except
the last chunk that might contain less).

If your ``bigfile.txt`` contains less than 100,000 lines, you can make the chunk
size smaller. The more chunks you have, the more processes you can run in
parallel. However, since launching a new process is not free, you shouldn't make
the chunks too small.

2. Distribute chunks to the cluster
-----------------------------------

In theory you could use the chunks in the ``bigtxt`` directory
directly, but in practice it is a good idea to distribute the IO load
to many separate servers.  Disco provides a utility script called
``distrfiles.py`` that distributes files from a directory to the cluster.

The script requires that the environment variable ``DISCO_ROOT``, which
is the home directory of Disco, is specified. It is usually defined in
``/etc/disco/disco.conf``, so you can export it to your shell by saying::

        source /etc/disco/disco.conf

By default, ``DISCO_ROOT=/srv/disco/``. The script will copy files to the
``$DISCO_ROOT/data/bigtxt`` directory.

Local cluster
'''''''''''''

Run the script as follows::

        python disco/util/distrfiles.py bigtxt /etc/nodes > bigtxt.chunks

Here ``bigtxt`` refers to the directory that contains the files and
``/etc/nodes`` is a file that lists available nodes in the cluster, one
hostname per line. The script copies files to the nodes randomly, and
outputs location of each file to the standard output, which we capture
here to the file ``bigtxt.chunks``. Take a look at that file to get an
idea how inputs are specified for Disco.

If you want to repeat this command multiple times, e.g. for a new set of
chunks, you need to run::

        REMOVE_FIRST=1 python disco/util/distrfiles.py bigtxt /etc/nodes > bigtxt.chunks

which first removes the target directory on each node before copying
the chunks. 

Amazon EC2
''''''''''

With Amazon EC2, we need to specify the ssh-key to the script, using the
``SSH_KEY`` environment variable, so it can copy files to the nodes. Since
the EC2's ssh-key is specific to the root user, we also need to set the
user to root with the ``SSH_USER`` variable. The ``DISCO_ROOT`` variable should
be set to its default value, ``/srv/disco``.

Run the script as follows::
        
        DISCO_ROOT=/srv/disco SSH_KEY=your-key-file SSH_USER=root python disco/util/distrfiles.py bigtxt ec2-nodes > bigtxt.chunk

Here ``your-key-file`` should be the same keypair file that was
used in :ref:`ec2setup`. The node list ``ec2-nodes`` is produced by
``setup-instances.py`` script. Similarly to the local clusters, you can
use the ``REMOVE_FIRST`` flag if you run the script many times with the
same dataset.


3. Write job functions
----------------------

Next we need to write map and reduce functions to count the words in
the chunks.

Start your favorite text editor and open a file called, say,
``count_words.py``. Let's write first our map function::

        def fun_map(e, params):
                return [(w, 1) for w in e.split()]

Quite compact, eh? The map function always takes two parameters, here they
are called *e* and *params*. The first parameter contains an input entry,
which is by default a line of input. An input entry can be anything, as
you can define a custom function that extracts them from an input stream
--- see the parameter *map_reader* in :func:`disco.core.Job` for more
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
                                stats[word] += int(count)
                        else:
                                stats[word] = int(count)
                for word, total in stats.iteritems():
                        out.add(word, total)

The reduce function takes three parameters: The first parameter, *iter*,
is an iterator that loops through the intermediate values produced by
the map function, which belong to this reduce instance or partition.

In this case, different words are randomly assigned to different reduce
instances. Again, this is something that can be changed --- see the
parameter *partition* in :func:`disco.core.Job` for more information. However,
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
words in parallel.

4. Run the job
--------------

Now the only thing missing is a command for running the job. First,
we establish a connection to the Disco master by instantiating a
:class:`disco.core.Disco` object. After that, we can start the job by
calling :meth:`disco.core.Disco.new_job`. There's a large number of
parameters that you can use to specify your job but only three of them
are required for a simple job like ours.

In addition to starting the job, we want to print out the results as well.
First, however, we have to wait until the job has finished. This is done with
the :meth:`disco.core.Disco.wait` call, which returns results of the job once
has it has finished. For convenience, the :meth:`disco.core.Disco.wait` method, 
as well as other methods related to a job, can be called through the
:class:`disco.core.Job` object that is returned by :meth:`disco.core.Disco.new_job`.

A function called :func:`disco.core.result_iterator` takes
a list of addresses to the result files, that is returned by
:meth:`disco.core.Disco.wait`, and iterates through all key-value pairs
in the results.

The following lines run the job and print out the results. Write them to the end
of your file::

        import sys
        from disco.core import Disco, result_iterator
        
        results = Disco(sys.argv[1]).new_job(
                name = "disco_tut",
                input = sys.argv[2:],
                map = fun_map,
                reduce = fun_reduce).wait()
        
        for word, total in result_iterator(results):
                print word, total

Here we read the address of the Disco master and the input files from
the command line. Note how the map and reduce functions are provided to
:meth:`disco.core.Disco.new_job` simply as normal keywords arguments *map*
and *reduce*.

Now comes the moment of truth. 

Local cluster
'''''''''''''
        
Run the script as follows::

        python count_words.py disco://localhost `cat bigtxt.chunks` > bigtxt.results

If you run the Disco master in a non-standard port, replace
``disco://localhost`` with the correct address to the
master.

Amazon EC2
''''''''''

In contrast to a local cluster, :func:`disco.core.result_iterator`
can't fetch the results directly from the EC2 nodes. Due to this reason, we must
use the master node as a proxy. 

Run the scripts as follows::
        
        DISCO_PROXY=disco://localhost python count_words.py disco://localhost `cat bigtxt.chunks` > bigtxt.results

Here we assume that there's a SSH tunnel from your local machine to the
EC2 master, as started automatically by the ``setup-instances.py`` script.

----

If everything goes well, the script pauses for some time while the
job executes. The inputs are read from the file ``bigtxt.chunks``
which was created earlier. Finally the outputs are written to
``bigtxt.results``.  While the job is running, you can point your web
browser at ``http://localhost:8989`` (or some other port where you run the
Disco master) which lets you follow the progress of your job in real-time.

You can also set the environment variable ``DISCO_EVENTS=1`` to see job 
events on your console instead of the web UI. 

What next?
----------

As you saw, creating a new Disco job is pretty straightforward. Next you could
write functions for a bit more complex job, which could, for instance, count
only words that are provided as a parameter to the map function.

You can also experiment with providing custom partitioning and reader
functions. They are written in the same way as map and reduce functions.
Just see some examples in the :mod:`disco.func` module. After that,
you could try to chain many map/reduce jobs together, so that outputs
of the previous job are used as the inputs for the next one --- in that
case you need to use :func:`disco.func.chain_reader`.

The best way to learn is to pick a problem or algorithm that you know
well, and implement it with Disco. After all, Disco was designed to
be as simple as possible so you can concentrate on your own problems,
not on the framework.

