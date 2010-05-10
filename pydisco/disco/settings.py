"""
:mod:`disco.settings` -- Disco Settings
=======================================

Settings can be specified in a file and/or using environment variables.
Possible settings are as follows:

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
"""

import os, socket

class DiscoSettings(dict):
    defaults = {
        'DISCO_SETTINGS':        "''",
        'DISCO_DEBUG':           "'off'",
        'DISCO_EVENTS':          "''",
        'DISCO_FLAGS':           "''",
        'DISCO_LOG_DIR':         "'/var/log/disco'",
        'DISCO_MASTER':          "'http://%s:%s' % (DISCO_MASTER_HOST, DISCO_PORT)",
        'DISCO_MASTER_HOME':     "'/usr/lib/disco'",
        'DISCO_MASTER_HOST':     "socket.gethostname()",
        'DISCO_NAME':            "'disco_%s' % DISCO_PORT",
        'DISCO_PID_DIR':         "'/var/run'",
        'DISCO_PORT':            "8989",
        'DISCO_ROOT':            "'/srv/disco'",
        'DISCO_USER':            "os.getenv('LOGNAME')",
        'DISCO_DATA':            "os.path.join(DISCO_ROOT, 'data')",
        'DISCO_MASTER_ROOT':     "os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)",
        'DISCO_CONFIG':          "os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)",
        'DISCO_WORKER':          "os.path.join(DISCO_HOME, 'node', 'disco-worker')",
        'DISCO_ERLANG':          "guess_erlang()",
        'DISCO_HTTPD':           "'lighttpd'",
        'DISCO_WWW_ROOT':        "os.path.join(DISCO_MASTER_HOME, 'www')",
        'PYTHONPATH':            "DISCO_PATH",
# GC
        'DISCO_GC_AFTER':        "3153600000",
# PROXY
        'DISCO_PROXY':           "''",
        'DISCO_PROXY_PORT':      "8999",
        'DISCO_PROXY_PID':       "os.path.join(DISCO_ROOT, '%s-proxy.pid' % DISCO_NAME)",
        'DISCO_PROXY_CONFIG':    "os.path.join(DISCO_ROOT, '%s-proxy.conf' % DISCO_NAME)",
# TESTING
        'DISCO_TEST_HOST':       "socket.gethostname()",
        'DISCO_TEST_PORT':       "9444",
        'DISCO_TEST_PURGE':      "'purge'",
# SCHEDULER
        'DISCO_SCHEDULER':       "'fair'",
        'DISCO_SCHEDULER_ALPHA': ".001",
# DDFS
        'DDFS_ENABLED':          "'on'",
        'DDFS_ROOT':             "os.path.join(DISCO_ROOT, 'ddfs')",
        'DDFS_PUT_PORT':         "8990",
        'DDFS_TAG_MIN_REPLICAS': "3",
        'DDFS_TAG_REPLICAS':     "3",
        'DDFS_BLOB_REPLICAS':    "3",
        'DDFS_PUT_MAX':          "3",
        'DDFS_GET_MAX':          "3"
        }

    must_exist = ('DISCO_DATA',
                  'DISCO_ROOT',
                  'DISCO_MASTER_HOME',
                  'DISCO_MASTER_ROOT',
                  'DISCO_LOG_DIR',
                  'DISCO_PID_DIR',
                  'DDFS_ROOT')

    def __init__(self, filename=None, **kwargs):
        super(DiscoSettings, self).__init__(kwargs)
        if filename:
            execfile(filename, {}, self)

    def __getitem__(self, key):
        if key in os.environ:
            return os.environ[key]
        if key not in self:
            return eval(self.defaults[key], globals(), self)
        return super(DiscoSettings, self).__getitem__(key)

    @property
    def env(self):
        settings = os.environ.copy()
        settings.update(dict((k, str(self[k])) for k in self.defaults))
        settings['DISCO_SETTINGS'] = ','.join(self.defaults)
        return settings

def guess_erlang():
    if os.uname()[0] == 'Darwin':
        return '/usr/libexec/StartupItemContext erl'
    return 'erl'
