
.. _troubleshooting:

Troubleshooting Disco installation
==================================

:ref:`setup` should tell you enough to get Disco up and running,
but it may happen that Disco doesn't work properly right after installation.
If you can't :ref:`run the word count example <insttest>` successfully,
the reason is usually a small misconfigured detail somewhere.
This document tries to help you figure out what's going wrong.

Since Disco is a distributed system based on loosely coupled components,
it is possible to debug the system by testing the components one by one.
This document describes the troubleshooting process.
It is intended to help you to get Disco working locally, on a single computer.
After you have done this, distributing it should be rather straightforward:
the same debugging techniques apply.

.. note:: It's assumed that you have already followed the steps in
          :ref:`install_source`.


.. _stopping_disco:

Make sure Disco is not running
------------------------------

If you have started Disco earlier,
try to stop the :term:`master` using ``disco stop``
(or :kbd:`C-c` if you are running with ``disco nodaemon``).
If you cannot seem to stop Disco this way, kill the ``beam`` processes by hand,

.. hint:: You can use
   ::

        ps aux | grep beam.*disco

   and::

        kill PID

   to hunt down and kill the :term:`pids <pid>`, respectively.

Is the master starting?
-----------------------

Start Disco by saying::

        disco nodaemon

If everything goes well,
you should see a bunch of ``=INFO REPORT=`` messages printed to the screen.
If you see any ``=ERROR REPORT=`` messages, something is wrong,
and you should try to resolve the particular issue :term:`Erlang` is reporting.
These messages often reveal what went wrong during the startup.

If you see something like this::

        application: disco
        exited: {bad_return,{{disco_main,start,[normal,[]]},
                {'EXIT',["Specify ",scgi_port]}}}

Disco is trying to start up properly,
but your Erlang installation probably doesn't work
correctly and you should try to re-install it.

.. note:: If you started disco using ``disco start``,
          you will have to check the logs in :envvar:`DISCO_LOG_DIR`
          for such messages.

          If you can't find the log file, the master didn't start at all.
          See if you can find master binaries in the ``ebin`` directory
          under :envvar:`DISCO_MASTER_HOME`.
          If there are no files there,
          check for compilation errors when you :ref:`install_source`.

.. hint:: If you don't know what :envvar:`DISCO_LOG_DIR` is
          (or any other :mod:`setting <disco.settings>`),
          you can check with::

                disco -v

If the master is running, you can proceed to the next step
(you can double check with ``ps`` as in :ref:`stopping_disco`).
If not, the master didn't start up properly.

Are there any nodes on the status page?
---------------------------------------

Now that we know that the master process is running,
we should be able to configure the system.
Open your web browser and go to
`http://localhost:8989/ <http://localhost:8989/>`_
(or whatever your :envvar:`DISCO_MASTER_HOST`
and :envvar:`DISCO_PORT` are set to).
The Disco status page should open.

Do you see any boxes with black title bars on the status page
(like `in this screenshot <../_static/screenshots/disco-main.png>`_)?
If not, add nodes to the system as instructed in :ref:`confignodes`.

If adding nodes through the web interface fails,
you can try editing the config file manually.
For instance,
if you replace :envvar:`DISCO_ROOT` in the following command,
it will create a configuration file with one node::

        echo '[["localhost", "1"]]' > DISCO_ROOT/disco_4441.config

.. hint:: Remember to restart the master after editing the config file by hand.

.. note::

    Note that as of version 0.3.1 of Disco, jobs can be submitted to
    Disco even if there are no nodes configured.  Disco assumes that
    this configuration is a temporary state, and some nodes will be
    added.  In the meantime, Disco retains the jobs, and will start or
    resume them once nodes are added to the configuration and become
    available.

Now is a good time to try to run a Disco :term:`job`.
Go ahead and retry the :ref:`installation test <insttest>`.
You should see the job appear on the Disco status page.
If the job succeeds, it should appear with a green box on the job list.
If it turns up red, we need to continue debugging.

Are slaves running?
-------------------

In addition to the master process on the master node,
:term:`Erlang` runs a :term:`slave` on each node in a Disco cluster.

Make sure that the slave is running::

        ps aux | grep -o disco.*slave@

If is is running, you should see something like this::

   disco_8989_master@discodev -sname disco_8989_slave@
   disco.*slave@

If you get a similar output, go to `Do workers run?`_. If not, read on.

Is SSH working?
'''''''''''''''

The most common reason for the slave not starting up is a problem with :term:`SSH`.
Try the following command::

        ssh localhost erl

If SSH asks for a password, or any other confirmation,
you need to configure SSH properly as instructed in
:ref:`authentication configuration <configauth>`.

If SSH seems to work correctly, Erlang should be able to start a slave.
Check that you get something similar when you do::

        [user@somehost dir]$ disco debug
        Erlang VERSION

        Eshell VERSION (abort with ^G)
        (testmaster@somehost)1> slave:start(localhost, "testnode").
        {ok,testnode@localhost}
        (testmaster@somehost)1> net_adm:ping(testnode@localhost).
        pong

If Erlang doesn't return ``{ok,_Node}`` for the first expression,
or if it returns ``pang`` for the second expression,
there's probably something wrong either with your
:ref:`authentication configuration <configauth>`.

.. note:: Node names need to be consistent.
          If your master node is called ``huey`` and your remote node ``dewey``,
          ``dewey`` must be able to connect to the master node named ``huey``,
          and vice versa.
          Aliasing is not allowed.

Is your firewall configured correctly?
--------------------------------------

Disco requires a number of ports to be accessible to function properly.

- 22 - SSH
- 8990 - DDFS web API
- 8989 - Disco web interface/API. Must be unblocked on slaves and the master.
- 4369 - Erlang port mapper
- 30000 to 65535 - Communication between Erlang slaves

.. note::
   Future versions of Disco may allow you to specify a port range for Erlang to
   use. However, the current version of Disco does not, so you must open up the
   entire port range.

Is your DNS configured correctly?
---------------------------------

Disco uses short DNS names of cluster nodes in its configuration.
Please ensure that short hostnames were entered in the
:ref:`confignodes` step, and that DNS resolves these short names
correctly across all nodes in the cluster.

Do workers run?
---------------

The :term:`master` is responsible for starting individual
processes that execute the actual :term:`map` and :term:`reduce`
:term:`tasks <task>`.
Assuming that the master is running correctly,
the problem might be in the :term:`worker`.

See what happens with the following command::

        ssh localhost "python DISCO_HOME/lib/disco/worker/classic/worker.py"

Where :envvar:`DISCO_HOME` in this case must be the Disco source directory.
It should start and send a message like this::

   WORKER 32 {"version": "1.0", "pid": 13492}

If you get something else, you may have a problem with your :envvar:`PATH`
or Python installation.

Still no success?
-----------------

If the problem persists, or you can't get one of the steps above working,
do not despair!
Report your problem to friendly Disco developers
:doc:`on IRC or the mailing list <getinvolved>`.
Please mention in your report the steps you followed and the results you got.
