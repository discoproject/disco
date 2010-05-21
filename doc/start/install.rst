
.. _setup:

Setting up Disco
================

This document helps you to install Disco either on a single server or a
cluster of servers. This requires installation of several packages, which
may or may not be totally straightforward. If you want to get something
working quickly, you should consider trying out Disco in Amazon EC2
(:ref:`ec2setup`) which requires no configuration on your side.

**Shortcut for Debian / Ubuntu users:** If you run Debian testing or
some recent version of Ubuntu on the AMD64 architecture, you may try
out our **experimental** deb-packages which are available at `Disco
download page <http://discoproject.org/download.html>`_. If you managed
to install the packages, you can skip over the steps 0-3 below and go
to :ref:`configauth` directly.


Background
----------

You should have a quick look at :ref:`overview` before setting up the
system, to get an idea what should go where and why. To make a long
story short, Disco works as follows:

 * Disco users start Disco jobs in Python scripts.
 * Jobs requests are sent over HTTP to the master.
 * Master is an Erlang process that receives requests over HTTP.
 * Master launches another Erlang process, worker supervisor, on each node over
   SSH.
 * Worker supervisors run Disco jobs as Python processes.
 * Data is accessed through HTTP using Lighttpd as HTTP server.

In the following we set up SSH, Erlang, Python, and Lighttpd to work
for Disco.

0. Prerequisites
----------------

You need at least one Linux/Unix server. Any distribution should work (including Mac OS X).

On each server the following applications / libraries are required:

 * `SSH daemon and client <http://www.openssh.com>`_
 * `Erlang/OTP R12B or newer <http://www.erlang.org>`_
 * `Lighttpd 1.4.17 or newer <http://lighttpd.net>`_
 * `Python 2.4 or newer <http://www.python.org>`_
 * `Python setuptools <http://pypi.python.org/pypi/setuptools>`_ (optional)
 * `cJSON module for Python <http://pypi.python.org/pypi/python-cjson>`_ (for Python < 2.6)

1. Install Disco
----------------

Download `the latest Disco package from discoproject.org
<http://discoproject.org/download.html>`_. Alternatively you can download `the
latest development snapshot from GitHub <http://github.com/tuulos/disco>`_.

Extract the package (if necessary) and ``cd`` into it.
We will refer to this directory as ``DISCO_HOME``.

If you want to install Disco locally, just run make::

        make

This is often the easiest and the least intrusive way to get started with Disco.

You should repeat the above command on all servers that belong to your
Disco cluster. Note that Disco should be found on the same path on all the servers.

To install system-wide, run make install as root::

        make install DESTDIR=/

This will build and install Disco to your system (see ``Makefile`` for exact
directories).

2. Prepare the runtime environment
----------------------------------

Next we need to perform the following tasks on all servers that belong
to the Disco cluster:

 * Create ``disco`` user (optional).
 * Check that the settings in ``settings.py`` are correct.

Often it is convenient to run Disco as a separate user.
Amongst other reasons, this allows setting user-specific
resource utilization limits for the Disco user (through ``limits.conf``
or similar mechanism). However, you can use any account for running
Disco. In the following, we refer to the user that runs ``disco-master``
as the Disco user.

Open ``DISCO_HOME/conf/settings.py``. This file sets a number of environment
variables that define the runtime environment for Disco.
Most likely you do not need to modify this file right away.
You can change the paths if the defaults are not suitable for your system.
See :mod:`disco.settings` for more information on the various settings and their default values.

3. Start Disco
--------------

Disco now uses a streamlined command-line interface (see :mod:`discocli`).
On the master node, start the Disco master by executing ``disco start``.

You can easily integrate ``disco`` into your system's startup sequence.
For instance, you can see how ``debian/disco-master.init`` and
``debian/disco-node.init`` are implemented in the Disco's ``debian``
branch.

If Disco has started up properly, you should see processes ``lighttpd``
and ``beam.smp`` running on your master node, and ``lighttpd`` on the
other servers.

An easy way to test if Disco is starting up properly is to run ``disco nodaemon``
instead of ``disco start``.
This will start the master node and bring you right to its Erlang shell,
without redirecting the log to a file.

.. _configauth:

4. Configure authentication
---------------------------

Next we need to enable passwordless login via ssh to all servers in
the Disco cluster. If you have only one machine, you need to enable
passwordless login to ``localhost`` for the Disco user.

Run the following command as the Disco user, assuming that it doesn't
have valid ssh-keys already::

        ssh-keygen -N '' -f ~/.ssh/id_dsa

If you have one server (or shared home directories), say::

        cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys

Otherwise, repeat the following command for all the servers ``nodeX``
in the cluster::

        ssh-copy-id nodeX

Now try to login to all servers in the cluster or ``localhost``, if you
have only one machine. You should not need to give a password nor answer
to any questions after the first login attempt.

As the last step, if you run Disco on many machines, you need to make
sure that all servers in the Disco cluster use the same Erlang cookie,
which is used for authentication between Erlang nodes. Run the following
command as the Disco user on the master server::

        scp ~/.erlang.cookie nodeX:

Repeat the command for all the servers ``nodeX``.

5. Add nodes to Disco
---------------------

At this point you should have Disco up and running. The final step
before testing the system is to specify which servers are available for
Disco. This is done on the Disco's web interface.

Point your browser at ``http://master:<DISCO_PORT>``, where ``master`` should be
replaced with the actual hostname of your machine or ``localhost``
if you run Disco locally or through an SSH tunnel.
The default port is ``8989``.

You should see the Disco main screen (see `a screenshot here
<http://discoproject.org/screenshots.html>`_). Click ``configure`` on
the right side of the page. On the configuration page, click ``add row``
to add a new set of available nodes. Click the cells on the new empty
row, and add hostname of an available server (or a range of hostnames,
see below) in the left cell and the number of available cores (CPUs)
on that server in the right cell. Once you have entered a value, click
the cell again to save it.

You can add as many rows as needed to fully specify your cluster, which may
have varying number of cores on different nodes. Click ``save table``
when you are done.

If you have only a single machine, the resulting table should look like
this, assuming that you have two cores available for Disco:

.. image:: ../images/config-localhost.png

If you run Disco in a cluster, you can specify multiple nodes on a single line,
if the nodes are named with a common prefix, as here:

.. image:: ../images/config-cluster.png

This table specifies that there are 30 nodes available in the cluster, from
``nx01`` to ``nx30`` and each node has 8 cores.

.. _insttest:

6. Test the system
------------------

Now Disco should be ready for use.

We can use the following simple Disco script that computes word
frequencies in `a text file <http://discoproject.org/chekhov.txt>`_
to see that the system works correctly. Copy the following code to a
file called ``count_words.py``::

        import sys
        from disco.core import Disco, result_iterator

        def fun_map(e, params):
            return [(w, 1) for w in e.split()]

        def fun_reduce(iter, out, params):
            s = {}
            for w, f in iter:
                s[w] = s.get(w, 0) + int(f)
            for w, f in s.iteritems():
                out.add(w, f)

        master = sys.argv[1]
        print "Starting Disco job.."
        print "Go to %s to see status of the job." % master
        results = Disco(master).new_job(
                        name = "wordcount",
                        input = ["http://discoproject.org/chekhov.txt"],
                        map = fun_map,
                        reduce = fun_reduce).wait()

        print "Job done. Results:"
        for word, frequency in result_iterator(results):
                print word, frequency

Run the script as follows::

        python count_words.py http://master:8989

Replace the address above with the same address you used to
configure Disco earlier. You must use the same version of Python for
running Disco scripts as you use on the server side.

You can run the script on any machine that can access Disco on the
specified address. The safest bet is to run the script on
the master node itself.

If the machine where you run the script can access the master node but
not other nodes in the cluster, you need to set the environment variable
``DISCO_PROXY=http://master:8989``. The proxy address should be the
same as the master's above. This makes Disco to fetch results through
the master node, instead of connecting to the nodes directly.

If the script produces some results, congratulations, you have a
working Disco setup! If you are new to Disco, you might want to read
:ref:`tutorial` next.

If the script fails, see the section about :ref:`troubleshooting`.
