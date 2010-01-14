
.. _settings:

Disco settings
==============

        *DISCO_DEBUG*
                Sets the debugging level for Disco.
                Default is 1.

        *DISCO_EVENTS*
                If set, events are logged to `stdout`.
                If set to ``json``, events will be written as JSON strings.
                If set to ``nocolor``, ANSI color escape sequences will not be used, even if the terminal supports it.

        *DISCO_FLAGS*
                Default is the empty string.

        *DISCO_LOG_DIR*
                Directory where log-files are created.
                The same path is used for all nodes in the cluster.
                Default is ``/var/log/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        *DISCO_MASTER_HOME*
                Directory containing the Disco ``master`` directory.
                Default is ``/usr/lib/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        *DISCO_MASTER_HOST*
                The hostname of the master.
                Default obtained using ``socket.gethostname()``.

        *DISCO_NAME*
                A unique name for the Disco cluster.
                Default obtained using ``'disco_%s' % DISCO_SCGI_PORT``.

        *DISCO_PID_DIR*
                Directory where pid-files are created.
                The same path is used for all nodes in the cluster.
                Default is ``/var/run``.
                ``conf/settings.py.template`` provides a reasonable override.

        *DISCO_PORT*
                The port the workers use for `HTTP` communication.
                Default is 8989.

        *DISCO_ROOT*
                Root directory for Disco-written data and metadata.
                Default is ``/srv/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        *DISCO_SCGI_PORT*
                The port the Disco master uses for `SCGI` communication.
                Default is 4444.

        *DISCO_ULIMIT*
                *Temporarily unsupported*.
                Default is 16000000.

        *DISCO_USER*
                The user Disco should run as.
                Default obtained using ``os.getenv(LOGNAME)``.

        *DISCO_DATA*
                Directory to use for writing data.
                Default obtained using ``os.path.join(DISCO_ROOT, data)``.

        *DISCO_MASTER_ROOT*
                Directory to use for writing master data.
                Default obtained using ``os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)``.

        *DISCO_CONFIG*
                Directory to use for writing cluster configuration.
                Default obtained using ``os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)``.

        *DISCO_LOCAL_DIR*
                Default obtained using ``os.path.join(DISCO_ROOT, local, '_%s' % DISCO_NAME)``.

        *DISCO_WORKER*
                Executable which launches the Disco worker process.
                Default obtained using ``os.path.join(DISCO_HOME, node, disco-worker)``.

        *DISCO_TEST_HOST*
                The hostname that the test data server should bind on.
                Default is ``DISCO_MASTER_HOST``.

        *DISCO_TEST_PORT*
                The port that the test data server should bind to.
                Default is 9444.

        *DISCO_ERLANG*
                Command used to launch Erlang on all nodes in the cluster.
                Default usually ``erl``, but depends on the OS.

        *DISCO_HTTPD*
                Command used to launch `lighttpd`.
                Default is ``lighttpd``.

        *DISCO_WWW_ROOT*
                Directory that is the document root for the master `HTTP` server.
                Default obtained using ``os.path.join(DISCO_MASTER_HOME, www)``.

        *DISCO_SCHEDULER*
                The type of scheduler that disco should use.
                The only options are `fair` and `fifo`.
                Default is ``fair``.

        *DISCO_SCHEDULER_ALPHA*
                Parameter controlling how much the ``fair`` scheduler punishes long-running jobs vs. short ones.
                Default is .001 and should usually not need to be changed.
