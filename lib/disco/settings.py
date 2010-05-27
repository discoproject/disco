"""
:mod:`disco.settings` -- Disco Settings
=======================================

Settings can be specified in a Python file and/or using environment variables.
Settings specified in environment variables override those stored in a file.
The default settings are intended to make it easy to get Disco running on a single node.
:command:`make install` will create a more reasonable settings file for a cluster environment,
and put it in ``/etc/disco/settings.py``

Disco looks in the following places for a settings file:

        - The settings file specified using the command line utility
          ``--settings`` option.
        - ``~/.disco``
        - ``/etc/disco/settings.py``

Possible settings for Disco are as follows:

        :envvar:`DISCO_DATA`
                Directory to use for writing data.
                Default obtained using ``os.path.join(DISCO_ROOT, data)``.

        :envvar:`DISCO_DEBUG`
                Sets the debugging level for Disco.
                Default is 1.

        :envvar:`DISCO_ERLANG`
                Command used to launch Erlang on all nodes in the cluster.
                Default usually ``erl``, but depends on the OS.

        :envvar:`DISCO_EVENTS`
                If set, events are logged to `stdout`.
                If set to ``json``, events will be written as JSON strings.
                If set to ``nocolor``, ANSI color escape sequences will not be used, even if the terminal supports it.

        :envvar:`DISCO_FLAGS`
                Default is the empty string.

        :envvar:`DISCO_HOME`
                The directory which Disco runs out of.
                If you run Disco out of the source directory,
                you shouldn't need to change this.
                If you use ``make install`` to install Disco,
                it will be set properly for you in ``/etc/disco/settings.py``.

        :envvar:`DISCO_HTTPD`
                Command used to launch `lighttpd`.
                Default is ``lighttpd``.

        :envvar:`DISCO_MASTER_HOME`
                Directory containing the Disco ``master`` directory.
                Default is ``/usr/lib/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        :envvar:`DISCO_MASTER_HOST`
                The hostname of the master.
                Default obtained using ``socket.gethostname()``.

        :envvar:`DISCO_MASTER_ROOT`
                Directory to use for writing master data.
                Default obtained using ``os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)``.

        :envvar:`DISCO_MASTER_CONFIG`
                Directory to use for writing cluster configuration.
                Default obtained using ``os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)``.

        :envvar:`DISCO_NAME`
                A unique name for the Disco cluster.
                Default obtained using ``'disco_%s' % DISCO_PORT``.

        :envvar:`DISCO_LOG_DIR`
                Directory where log-files are created.
                The same path is used for all nodes in the cluster.
                Default is ``/var/log/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        :envvar:`DISCO_PID_DIR`
                Directory where pid-files are created.
                The same path is used for all nodes in the cluster.
                Default is ``/var/run``.
                ``conf/settings.py.template`` provides a reasonable override.

        :envvar:`DISCO_PORT`
                The port the workers use for `HTTP` communication.
                Default is 8989.

        :envvar:`DISCO_ROOT`
                Root directory for Disco-written data and metadata.
                Default is ``/srv/disco``.
                ``conf/settings.py.template`` provides a reasonable override.

        :envvar:`DISCO_USER`
                The user Disco should run as.
                Default obtained using ``os.getenv(LOGNAME)``.

        :envvar:`DISCO_WORKER`
                Executable which launches the Disco worker process.
                Default obtained using ``os.path.join(DISCO_HOME, node, disco-worker)``.

        :envvar:`DISCO_WWW_ROOT`
                Directory that is the document root for the master `HTTP` server.
                Default obtained using ``os.path.join(DISCO_MASTER_HOME, www)``.

        :envvar:`DISCO_GC_AFTER`
                How long to wait before garbage collecting data.
                Only results explictly saved to DDFS won't be garbage collected.
                Default is ``100 * 365 * 24 * 60 * 60`` (100 years).

Settings to control the proxying behavior:

        :envvar:`DISCO_PROXY_ENABLED`
                If set, enable proxying through the master.
                Default is ``''``.

        :envvar:`DISCO_PROXY`
                The address of the proxy to use on the client side.
                Default is ``''``.

        :envvar:`DISCO_PROXY_PORT`
                The port the master proxy should run on.
                Default is ``8999``.

Settings to control the scheduler behavior:

        :envvar:`DISCO_SCHEDULER`
                The type of scheduler that disco should use.
                The only options are `fair` and `fifo`.
                Default is ``fair``.

        :envvar:`DISCO_SCHEDULER_ALPHA`
                Parameter controlling how much the ``fair`` scheduler punishes long-running jobs vs. short ones.
                Default is .001 and should usually not need to be changed.

Settings used by the testing environment:

        :envvar:`DISCO_TEST_HOST`
                The hostname that the test data server should bind on.
                Default is ``DISCO_MASTER_HOST``.

        :envvar:`DISCO_TEST_PORT`
                The port that the test data server should bind to.
                Default is 9444.

Settings used by DDFS:

        :envvar:`DDFS_ROOT`
                The root data directory for DDFS.
                Default is ``os.path.join(DISCO_ROOT, 'ddfs')``.

        :envvar:`DDFS_PUT_PORT`
                The port to use for writing to DDFS nodes.
                Must be open to the Disco client unless proxying is used.
                Default is ``8990``.

        :envvar:`DDFS_PUT_MAX`
                The maximum default number of retries for a `PUT` operation.
                Default is ``3``.

        :envvar:`DDFS_GET_MAX`
                The maximum default number of retries for a `GET` operation.
                Default is ``3``.

The following settings are used by DDFS to determine the number of replicas for data/metadata to keep
(it is not recommended to use the provided defaults in a multinode cluster):

        :envvar:`DDFS_TAG_MIN_REPLICAS`
                The minimum number of replicas for a tag operation to succeed.
                Default is ``1``.

        :envvar:`DDFS_TAG_REPLICAS`
                The number of replicas of tags that DDFS should aspire to keep.
                Default is ``1``.

        :envvar:`DDFS_BLOB_REPLICAS`
                The number of replicas of blobs that DDFS should aspire to keep.
                Default is ``1``.
"""
import os, socket

from clx.settings import Settings

class DiscoSettings(Settings):
    defaults = {
        'DISCO_DATA':            "os.path.join(DISCO_ROOT, 'data')",
        'DISCO_DEBUG':           "'off'",
        'DISCO_ERLANG':          "guess_erlang()",
        'DISCO_EVENTS':          "''",
        'DISCO_FLAGS':           "''",
        'DISCO_HOME':            "guess_home()",
        'DISCO_HTTPD':           "'lighttpd'",
        'DISCO_MASTER':          "'http://%s:%s' % (DISCO_MASTER_HOST, DISCO_PORT)",
        'DISCO_MASTER_HOME':     "os.path.join(DISCO_HOME, 'master')",
        'DISCO_MASTER_HOST':     "socket.gethostname()",
        'DISCO_MASTER_ROOT':     "os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)",
        'DISCO_MASTER_CONFIG':   "os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)",
        'DISCO_NAME':            "'disco_%s' % DISCO_PORT",
        'DISCO_LIB':             "os.path.join(DISCO_HOME, 'lib')",
        'DISCO_LOG_DIR':         "os.path.join(DISCO_HOME, 'log')",
        'DISCO_PID_DIR':         "os.path.join(DISCO_HOME, 'run')",
        'DISCO_PORT':            "8989",
        'DISCO_ROOT':            "os.path.join(DISCO_HOME, 'root')",
        'DISCO_SETTINGS':        "''",
        'DISCO_SETTINGS_FILE':   "guess_settings()",
        'DISCO_ULIMIT':          "16000000",
        'DISCO_USER':            "os.getenv('LOGNAME')",
        'DISCO_WORKER':          "os.path.join(DISCO_HOME, 'node', 'disco-worker')",
        'DISCO_WWW_ROOT':        "os.path.join(DISCO_MASTER_HOME, 'www')",
        'PYTHONPATH':            "DISCO_LIB",
# GC
        'DISCO_GC_AFTER':        "100 * 365 * 24 * 60 * 60",
# PROXY
        'DISCO_PROXY_ENABLED':   "''",
        'DISCO_PROXY':           "''",
        'DISCO_PROXY_PORT':      "8999",
        'DISCO_PROXY_PID':       "os.path.join(DISCO_ROOT, '%s-proxy.pid' % DISCO_NAME)",
        'DISCO_PROXY_CONFIG':    "os.path.join(DISCO_ROOT, '%s-proxy.conf' % DISCO_NAME)",
# SCHEDULER
        'DISCO_SCHEDULER':       "'fair'",
        'DISCO_SCHEDULER_ALPHA': ".001",
# TESTING
        'DISCO_TEST_HOST':       "socket.gethostname()",
        'DISCO_TEST_PORT':       "9444",
        'DISCO_TEST_PROFILE':    "''",
        'DISCO_TEST_PURGE':      "'purge'",
# DDFS
        'DDFS_ROOT':             "os.path.join(DISCO_ROOT, 'ddfs')",
        'DDFS_PUT_PORT':         "8990",
        'DDFS_PUT_MAX':          "3",
        'DDFS_GET_MAX':          "3",
        'DDFS_TAG_MIN_REPLICAS': "1",
        'DDFS_TAG_REPLICAS':     "1",
        'DDFS_BLOB_REPLICAS':    "1",
        }

    globals = globals()

    must_exist = ('DISCO_DATA',
                  'DISCO_ROOT',
                  'DISCO_MASTER_HOME',
                  'DISCO_MASTER_ROOT',
                  'DISCO_LOG_DIR',
                  'DISCO_PID_DIR',
                  'DDFS_ROOT')

    settings_file_var = 'DISCO_SETTINGS_FILE'

    @property
    def env(self):
        settings = os.environ.copy()
        settings.update(dict((k, str(self[k])) for k in self.defaults))
        settings['DISCO_SETTINGS'] = ','.join(self.defaults)
        return settings

    def ensuredirs(self):
        for name in self.must_exist:
            self.safedir(name)

def guess_erlang():
    if os.uname()[0] == 'Darwin':
        return '/usr/libexec/StartupItemContext erl'
    return 'erl'

def guess_home():
    from disco.error import DiscoError
    disco_lib  = os.path.dirname(os.path.realpath(__file__))
    disco_home = os.path.dirname(os.path.dirname(disco_lib))
    if os.path.exists(os.path.join(disco_home, '.disco-home')):
        return disco_home
    raise DiscoError("DISCO_HOME is not specified, where should Disco live?")

def guess_settings():
    for settings_file in (os.path.expanduser('~/.disco'),
                          '/etc/disco/settings.py'):
        if os.path.exists(settings_file):
            return settings_file
    return ''
