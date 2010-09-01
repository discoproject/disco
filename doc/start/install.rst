
.. _setup:

Setting up Disco
================

This document helps you to install Disco either on a single server or a
cluster of servers. This requires installation of several packages, which
may or may not be totally straightforward.

**Shortcut for Debian / Ubuntu users:** If you run Debian testing or
some recent version of Ubuntu on the AMD64 architecture, you may try
out our **experimental** deb-packages which are available at the :doc:`Disco
download page <download>`. If you managed to install the packages,
you can skip over the steps 0-3 below and go to :ref:`configauth` directly.


Background
----------

You should have a quick look at :ref:`overview` before setting up the
system, to get an idea what should go where and why. To make a long
story short, Disco works as follows:

 * Disco users start Disco jobs in Python scripts.
 * Jobs requests are sent over HTTP to the master.
 * Master is an Erlang process that receives requests over HTTP.
 * Master launches another Erlang process, the worker supervisor, on each node over
   SSH.
 * Worker supervisors run Disco jobs as Python processes.

In the following we set up SSH, Erlang, Python to work for Disco.

0. Prerequisites
----------------

You need at least one Linux/Unix server. Any distribution should work (including Mac OS X).

On each server the following applications / libraries are required:

 * `SSH daemon and client <http://www.openssh.com>`_
 * `Erlang/OTP R13B or newer <http://www.erlang.org>`_
 * `Python 2.5 or newer <http://www.python.org>`_
 * `cJSON module for Python <http://pypi.python.org/pypi/python-cjson>`_ (for Python < 2.6)

Optionally, ``DISCO_PROXY`` needs one of

 * `Lighttpd 1.4.17 or newer <http://lighttpd.net>`_
 * `Varnish 2.1.3 or newer <http://varnish-cache.org>`_

1. Install Disco
----------------

Download :doc:`a recent version of Disco <download>`.

Extract the package (if necessary) and ``cd`` into it.
We will refer to this directory as ``DISCO_HOME``.

If you want to install Disco locally, just run make::

        make

This is often the easiest and the least intrusive way to get started with Disco.

You should repeat the above command on all servers that belong to your
Disco cluster. Note that Disco should be found on the same path on all the servers.
Alternatively, you can use a (NFS) shared home directory on all the nodes, which
makes development really straightforward.

To install system-wide, run make install as root::

        make install

This will build and install Disco to your system (see ``Makefile`` for exact
directories).

.. note::

    ``make install`` installs a configuration file to
    ``/etc/disco/settings.py`` that is tuned for clusters, not a single
    machine.

    By default, the settings assume that you have at least three nodes in your
    cluster, so DDFS can use three-way replication. If you have fewer nodes,
    you need to lower the number of replicas in ``/etc/disco/settings.py``::

        DDFS_TAG_MIN_REPLICAS=1
        DDFS_TAG_REPLICAS=1
        DDFS_BLOB_REPLICAS=1

    See :mod:`disco.settings` for more information.

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

If you have run ``make install``, open ``/etc/disco/settings.py``. This
file sets a number of environment variables that define the runtime
environment for Disco.  Most likely you do not need to modify this
file right away (however, see the note above on replication).  You can
change the paths if the defaults are not suitable for your system.
See :mod:`disco.settings` for more information on the various settings
and their default values.

3. Start Disco
--------------

Disco now uses a streamlined command-line interface (see :mod:`discocli`).
On the master node, start the Disco master by executing ``disco start``.

You can easily integrate ``disco`` into your system's startup sequence.
For instance, you can see how ``debian/disco-master.init`` and
``debian/disco-node.init`` are implemented in Disco's ``debian``
branch.

If Disco has started up properly, you should see ``beam.smp`` running on your
master node.

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
sure that all servers in the Disco cluster use the same `Erlang cookie`_,
which is used for authentication between Erlang nodes. Run the following
command as the Disco user on the master server::

        scp ~/.erlang.cookie nodeX:

Repeat the command for all the servers ``nodeX``.

.. _Erlang cookie: http://www.erlang.org/doc/getting_started/conc_prog.html#id2264467

.. _confignodes:

5. Add nodes to Disco
---------------------

At this point you should have Disco up and running. The final step
before testing the system is to specify which servers are available for
Disco. This is done on the Disco's web interface.

Point your browser at ``http://master:<DISCO_PORT>``, where ``master`` should be
replaced with the actual hostname of your machine or ``localhost``
if you run Disco locally or through an SSH tunnel.
The default port is ``8989``.

You should see the Disco main screen (see :ref:`a screenshot <screenshots>`).
Click ``configure`` on the right side of the page.
On the configuration page, click ``add row`` to add a new set of available nodes.
Click the cells on the new empty row, and add hostname of an available server
(or a range of hostnames, see below) in the left cell and the number of
available cores (CPUs) on that server in the right cell.
Once you have entered a value, click the cell again to save it.

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
frequencies in `a text file <http://discoproject.org/media/text/chekhov.txt>`_
to see that the system works correctly.

.. literalinclude:: ../../examples/util/count_words.py
   :language: python

Run the script as follows from ``DISCO_HOME``::

        python examples/utils/count_words.py

Disco attempts to use the current hostname as ``DISCO_MASTER_HOST`` if it is not
defined in any settings file.

If you are runing Disco on multiple machines you must use the same version of
Python for running Disco scripts as you use on the server side.

You can run the script on any machine that can access Disco on the configured
``DISCO_MASTER_HOST``. The safest bet is to run the script on the master node
itself.

If the machine where you run the script can access the master node but
not other nodes in the cluster, you need to set the environment variable
``DISCO_PROXY=http://master:8989``. The proxy address should be the
same as the master's above. This makes Disco fetch results through
the master node, instead of connecting to the nodes directly.

If the script produces some results, congratulations, you have a
working Disco setup! If you are new to Disco, you might want to read
:ref:`tutorial` next.

If the script fails, see the section about :ref:`troubleshooting`.
