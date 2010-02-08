
.. _disco:

=====
disco
=====

-----------------------
control a Disco cluster
-----------------------

SYNOPSIS
========

::

        disco [-v] [-s settings] -p
        disco [-v] [-s settings] [master|worker] [start|stop|restart|status]
        disco [-v] [-s settings] master nodaemon
        disco [-v] [-s settings] debug
        disco [-v] [-s settings] test [test_names]

DESCRIPTION
===========

`disco` is a fully-Python startup/configuration script which supports several exciting features.
The new startup script makes it even easier to get up and running with a Disco cluster.

.. note::

   This is the manpage for the ``disco`` command.
   Please see :ref:`setup` for more information on installing Disco.

.. hint::

   The documentation assumes that the executable ``$DISCO_HOME/bin/disco`` is on your system path.
   If it is not on your path, you can add it::

        ln -s $DISCO_HOME/bin/disco /usr/local/bin

   If ``/usr/local/bin`` is not in your ``$PATH``, use an appropriate replacement.
   Doing so allows you to simply call the command ``disco``, instead of specifying the complete path.

OPTIONS
=======

        -s settings, --settings settings
                        Read configuration settings from settings.
                        Default is ``$DISCO_HOME/conf/settings.py``.
        -v, --verbose
                        Print debugging messages.
        -p, --print-env
                        Print the effective Disco environment and exit.


COMMANDS
========

(**master** | **worker**) **start** | **stop** | **restart** | **status**
        Start, stop, restart or get the status of the master or a worker

**master nodaemon**
        Starts the master without daemonizing.
        The Erlang shell is opened and log messages are printed to stdout.

**debug**
        Opens a remote Erlang shell to the Disco master.

**test** [test_names]
        Runs the Disco test suite.
        Assumes Disco master is already running and configured.
        Test names is an optional list of names of modules in the ``tests`` directory (e.g. ``test_simple``).
        Test names may also include the names of specific test cases (e.g. ``test_sort.MemorySortTestCase``).

SEE ALSO
========

:ref:`settings`
