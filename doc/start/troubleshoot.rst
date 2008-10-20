
.. _troubleshooting:

Troubleshooting Disco installation
==================================

Sometimes it happens that Disco doesn't work properly right after
installation. Since Disco is a distributed system which needs several
separate components to work properly, finding out the culprit often
requires testing the components one by one. This page explains the
troubleshooting process.

First you should make sure that you can get Disco working locally, on
a single computer. After this, getting the distributed version to work
is rather straightforward.

Here we assume that you have installed Disco locally with the following steps:

 * Downloaded the latest source package from `discoproject.org <http://discoproject.org/download.html>`_ or from the `GitHub repository <http://github.com/tuulos/disco>`_.
 * Extracted the package to your home directory, say ``~/disco/``.
 * Compiled the sources by writing ``make`` in ``~/disco/``. 
 * Started Disco with ``~/disco/conf/start-master`` and ``~/disco/conf/start-node``.
 * Tried to run the ``count_words.py`` example from :ref:`setup` by saying ``python count_words.py http://localhost:7000``.

but the script crashed and/or didn't produce any results.

Let's find out what goes wrong. Follow the next steps in order.

0. Start Disco locally
----------------------

If you have started Disco earlier, kill the previous processes with
``killall -9 lighttpd`` and ``killall -9 beam`` (or ``killall -9
beam.smp`` if you have a multi-core system). Note that this kills all
Erlang and Lighttpd processes owned by the user, so don't do this if
you have some other Erlang / Lighttpd processes running besides Disco.

Then, restart Disco by saying::

        cd ~/disco
        conf/start-master
        conf/start-node

Here we assume that you've extracted Disco under ``~/disco``. If you
extracted Disco to some other directory, use it instead. Now you should
have Disco running locally.

1. Is Disco master running?
---------------------------

The first thing to check is to see that the master process has actually
started up. Say::

        ps aux | grep beam

If the master is running, you should see something like this::

        disco 6848 0.0 0.7 317220 125960 ? Sl Oct07 3:46
        /usr/lib/erlang/erts-5.6.3/bin/beam.smp -K true -- -root
        /usr/lib/erlang -progname erl -- -home /srv/disco -heart -noshell
        -sname disco_4444_master -rsh ssh -pa /usr/lib/disco//ebin
        -boot /usr/lib/disco//disco -kernel error_logger {file,
        "/var/log/disco//disco_4444.log"} -eval [handle_job,
        handle_ctrl] -disco disco_name "disco_4444" -disco disco_root
        "/srv/disco//data/_disco_4444" -disco disco_master_host "" -disco
        disco_slaves_os "linux" -disco scgi_port 4444 -disco disco_config
        "/srv/disco//disco_4444.config

If you do see output like above, the master is running correctly and
you can proceed to the next step.

If not, the master didn't start up properly. See if you can find a
log file at ``~/disco/disco_*.log``. If the file exists, see the last
messages in the file: They often reveal what went wrong during the
startup. A typical problem is a faulty configuration file, in which
case the last message is something like ``Opening config file`` in
the log file. In this case remove the faulty config file at
``~/disco/root/disco_*config"``.

If you can't find the log file, the master didn't start at all. See
if you can find master binaries at ``~/disco/master/ebin/*.ebin``. If
no such files exist, run ``cd ~/disco; make`` again and see if the
compilation fails for some reason.

If the binaries exist but the master doesn't start, run::

        cd ~/disco
        erl -boot master/disco

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
`http://localhost:7000/ <http://localhost:7000/>_`. The Disco status
page should open.

Do you see any boxes with black title bars on the status page (like `in
this screenshot <http://discoproject.org/img/disco-main.png>_`)? If not,
add nodes to the system as instructed in :ref:`setup`, setup 5.

If adding nodes through the web interface fails, it may be a bug in
Disco. In that case you can edit the config file manually. For instance,
the following command initializes a configuration file with one node::

        echo '[["localhost", "1"]]' > ~/disco/root/disco_4441.config

Remeber to restart the master after editing the config file by hand, as
instructed in the step 0. above.

3. Is worker supervisor running?
--------------------------------

Now is a good time to try to run a Disco job. Copy the ``count_words.py``
example from :ref:`setup` and run it by saying ``python count_words.py
http://localhost:7000``. You should see the job appear on the Disco
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

If you see something like this, go to step 4. If not, read on.

The most common reason for the supervisor not to start up, is a problem
with ssh authentication. Try the following command::

        ssh localhost erl

If ssh asks for a password, or any other confirmation, you need to
configure ssh properly as instructed in :ref:`setup` in section 4.

If ssh seems to work correctly, you should check that the Erlang's
``slave`` module works correctly. We can check it as follows::

        erl -rsh ssh -sname testmaster

        Erlang (BEAM) emulator...

        (testmaster@somehost)1> slave:start(localhost, "testnode").
        {ok,testnode@localhost}
        (testmaster@somehost)1> net_adm:ping(testnode@localhost).
        pong

If Erlang doesn't return ``{ok..`` for the first expression or if it
returns ``pang`` for the second expression, there's something wrong
either with your ssh or Erlang configuration. In this case your problem
is not specific to Disco.

4. Does disco-worker start up?
------------------------------

The worker supervisor in Disco is responsible for starting individual
Python processes that execute the actual jobs. Since we know that the
supervisor itself starts up correctly, the problem might be in the
``disco-worker`` Python process.

See what happens with the following command::

        ssh localhost "PATH=~/disco/node PYTHONPATH=~/disco/node:~/disco/pydisco disco-worker"

It should respond with an error message that includes::

        ... Invalid command line. Usage: ...

If you get something else, you may have a problem with your PATH settings
or Python installation.

You can also see what exactly Disco tries to execute as follows::

        grep "Spawn cmd" ~/disco/disco_*.log 

You should see lines starting with ``Spawn cmd: nice -19 disco-worker...``. You
can copy-paste one of the lines and try to execute it by hand. This way you can
easily see how ``disco-worker`` fails.

5. Are Lighttpd instances running?
----------------------------------

If the Disco master, worker supervisors and ``disco-worker`` processes all
seem to work properly, there are not many more places that could fail. 

Disco uses HTTP for data transfer, so it needs a web server running
on each node. The web server is started by the ``conf/start-node``
command. You can make sure that the server is actually running by pointing
your browser at `http://localhost:8989/ <http://localhost:8989/>`_
which should show a default directory listing provided by the server. 

If the server doesn't respond, try to restart it by running
``conf/start-node`` again.

Note that when using Disco on a single computer, you really need two
separate web servers running, typically at ports 7000 (master) and 8989
(node).


Still no success?
-----------------

If the problem still persists, or you couldn't get one of the steps
above work correctly, do not feel desperate! Report your problem
to friendly Disco developers `either on IRC or to the mailing list
<http://discoproject.org/getinvolved.html>`_. Please mention in your
report the steps that you've tried and the results you got.

