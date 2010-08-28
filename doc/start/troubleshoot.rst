
.. _troubleshooting:

Troubleshooting Disco installation
==================================

Sometimes it happens that Disco doesn't work properly right after
installation. Since Disco is a distributed system which is based on
several separate loosely coupled components, it is possible to debug
the system by testing the components one by one. This page describes
the troubleshooting process.

This page helps you to get Disco working locally, on a single
computer. After this, getting the distributed version to work is rather
straightforward.

Here we assume that you have installed Disco locally with the following steps:

 * Downloaded :doc:`a recent version of Disco <download>`.
 * Extracted the package to your home directory, say ``~/disco/``.
 * Compiled the sources by writing ``make`` in ``~/disco/``.
 * Started Disco with ``disco start``.
 * Tried to run the ``count_words.py`` example from :ref:`setup` as follows ``python count_words.py http://localhost:8989``.

but the script crashed and/or didn't produce any results.

Let's find out what went wrong. Follow the next steps in order.

0. Start Disco locally
----------------------

If you have started Disco earlier, try to stop the master using ``disco stop``.
If you cannot seem to stop Disco, kill the previous processes with
``killall -9 lighttpd`` and ``killall -9 beam`` (or ``killall -9
beam.smp`` if you have a multi-core system). Note that this kills all
Erlang and Lighttpd processes owned by the user, so don't do this if
you have some other Erlang / Lighttpd processes running besides Disco.

Then, restart Disco by saying::

        cd ~/disco
        disco nodaemon

Here we assume that you've extracted Disco under ``~/disco``. If you
extracted Disco to some other directory, replace ``~/disco/`` with your
actual directory here and in the examples below.

These commands should be enough to get Disco up and running. If you
can't run ``count_words.py`` example successfully, the reason is usually
a small misconfigured detail somewhere. We will find it out below.

1. Is Disco master running?
---------------------------

The first thing to check is to see that the master process has actually
started up. Say::

        ps aux | grep beam

If the master is running, you should see something like this::

        disco 6848 0.0 0.7 317220 125960 ? Sl Oct07 3:46
        /usr/lib/erlang/erts-5.6.3/bin/beam.smp -K true -- -root
        /usr/lib/erlang -progname erl -- -home /srv/disco -heart -noshell
        -sname disco_8989_master -rsh ssh -pa /usr/lib/disco//ebin
        -boot /usr/lib/disco//disco -kernel error_logger {file,
        "/var/log/disco//disco_8989.log"} -eval [handle_job,
        handle_ctrl] -disco disco_name "disco_8989" -disco disco_root
        "/srv/disco//data/_disco_8989" -disco disco_master_host "" -disco
        disco_slaves_os "linux" -disco scgi_port 8989 -disco disco_config
        "/srv/disco//disco_8989.config

If you do see output like above, the master is running correctly and
you can proceed to the next step.

If not, the master didn't start up properly. See if you can find a log
file at ``DISCO_LOG_DIR/*.log``.
If you used the nodaemon command to startup the Disco master,
the log should be printed to stdout.
If the file exists, see the last messages in the file:
They often reveal what went wrong during the startup.

.. hint::
   If you do not know what DISCO_LOG_DIR should be,
   you can find out the environment Disco is using to run itself by running::

       disco -p


If you can't find the log file, the master didn't start at all. See
if you can find master binaries at ``DISCO_HOME/master/ebin/*.ebin``. If
no such files exist, run ``cd DISCO_HOME; make`` again and see if the
compilation fails for some reason.

If the binaries exist but the master doesn't start, run::

        cd ~/disco
        disco nodaemon

This assumes ``DISCO_HOME`` is ``~/disco``.
This command tries to start the Erlang virtual machine with the Disco
application. The command should fail with the following message::

        application: disco
        exited: {bad_return,{{disco_main,start,[normal,[]]},
                {'EXIT',["Specify ",scgi_port]}}}

which signals that Disco is trying to start up properly. If you don't
see anything like this, your Erlang installation probably doesn't work
correctly and you should try to re-install it.

2. Are there any nodes on the status page?
------------------------------------------

Now that we know that the master process is running, we should
be able to configure the system. Open your web browser and go to
`http://localhost:8989/ <http://localhost:8989/>`_. The Disco status
page should open.

Do you see any boxes with black title bars on the status page (like `in
this screenshot <../_static/screenshots/disco-main.png>`_)? If not,
add nodes to the system as instructed in :ref:`confignodes`.

If adding nodes through the web interface fails, it may be a bug in
Disco. In that case you can edit the config file manually. For instance,
the following command initializes a configuration file with one node::

        echo '[["localhost", "1"]]' > ~/disco/root/disco_4441.config

Remember to restart the master after editing the config file by hand::

         disco restart

3. Is worker supervisor running?
--------------------------------

Now is a good time to try to run a Disco job. Copy the ``count_words.py``
example from :ref:`setup` and run it by saying ``python count_words.py
http://localhost:8989``. You should see the job appear on the Disco
status page. If the job succeeds, it should appear with a green box on
the job list. If it turns up red, we need to continue debugging.

In addition to the master process, each node that runs Disco jobs needs
a worker supervisor (see :ref:`overview` for details). Make sure that
you have a supervisor running::

        ps aux | grep slave_waiter

If the supervisor is running, you should see something like this::

        disco 4594 1.1 3.7 8136 4672 ? Sl 21:45
        0:00 /usr/lib/erlang/erts-5.6.3/bin/beam -K true -- -root
        /usr/lib/erlang -progname erl -- -home /home/tuulos -noshell
        -noinput -noshell -noinput -master disco_4441_master@discodev
        -sname disco_4441_slave@localhost -s slave slave_start
        disco_4441_master@discodev slave_waiter_0 -pa
        /home/tuulos/src/disco/master//ebin

If you get a similar output, go to step 4. If not, read on.

The most common reason for the supervisor not starting up is a problem
with ssh authentication. Try the following command::

        ssh localhost erl

If ssh asks for a password, or any other confirmation, you need to
configure ssh properly as instructed in :ref:`configauth`.

If ssh seems to work correctly, you should check that the Erlang's
``slave`` module works correctly. You can check it as follows::

          disco debug

        Erlang (BEAM) emulator...

        (testmaster@somehost)1> slave:start(localhost, "testnode").
        {ok,testnode@localhost}
        (testmaster@somehost)1> net_adm:ping(testnode@localhost).
        pong

If Erlang doesn't return ``{ok..`` for the first expression or if it
returns ``pang`` for the second expression, there's something wrong either
with your ssh or Erlang configuration. You should double-check that
the Erlang security cookie at ``~/.erlang.cookie`` is the same on all
the nodes (see :ref:`configauth`). The cookie must be readable only to the
disco user, so run ``chmod 400 ~/.erlang.cookie`` on all the nodes.

Note that node names need to be consistent. If your master node is called
``huey`` and your remote node ``dewey``, ``dewey`` must be able to connect to
the master node by the name ``huey`` and vice versa. Aliasing is not allowed.

.. warning::
   Future versions of Disco may allow you to specify a port range for Erlang to use,
   however the current version of Disco does not,
   which may cause problems if you are using a firewall.
   If you have a firewall running inside your cluster,
   you may need to turn it off in order for Disco to work properly.

4. Does disco-worker start up?
------------------------------

The worker supervisor is responsible for starting individual Python
processes that execute the actual map and reduce functions. Assuming
that the supervisor is running correctly, the problem might be in the
``disco-worker`` Python process.

See what happens with the following command::

        ssh localhost "PATH=~/disco/node PYTHONPATH=~/disco/node:~/disco/lib disco-worker"

It should respond with an error message that includes::

        ... Invalid command line. Usage: ...

If you get something else, you may have a problem with your PATH settings
or Python installation.

You can find out what exactly Disco tries to execute as follows::

        grep "Spawn cmd" DISCO_LOG_DIR/master*.log

In the log, you should see lines starting with ``Spawn cmd: nice -19 disco-worker...``.
You can copy-paste one of the lines and try to execute it by hand.
This way you can easily see how ``disco-worker`` fails.

Still no success?
-----------------

If the problem persists, or you can't get one of the steps above working,
do not despair!
Report your problem to friendly Disco developers
:doc:`on IRC or the mailing list <getinvolved>`.
Please mention in your report the steps you followed and the results you got.

