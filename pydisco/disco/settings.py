import os, socket

class DiscoSettings(dict):
    defaults = {
        'DISCO_EVENTS':          "''",
        'DISCO_FLAGS':           "''",
        'DISCO_LOG_DIR':         "'/var/log/disco'",
        'DISCO_MASTER_HOME':     "'/usr/lib/disco'",
        'DISCO_MASTER_HOST':     "socket.gethostname()",
        'DISCO_NAME':            "'disco_%s' % DISCO_SCGI_PORT",
        'DISCO_PID_DIR':         "'/var/run'",
        'DISCO_PORT':            "8989",
        'DISCO_ROOT':            "'/srv/disco'",
        'DISCO_SCGI_PORT':       "4444",
        'DISCO_ULIMIT':          "16000000",
        'DISCO_USER':            "os.getenv('LOGNAME')",
        'DISCO_DATA':            "os.path.join(DISCO_ROOT, 'data')",
        'DISCO_MASTER_ROOT':     "os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)",
        'DISCO_CONFIG':          "os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)",
        'DISCO_LOCAL_DIR':       "os.path.join(DISCO_ROOT, 'local', '_%s' % DISCO_NAME)",
        'DISCO_WORKER':          "os.path.join(DISCO_HOME, 'node', 'disco-worker')",
        'DISCO_TEST_HOST':       "DISCO_MASTER_HOST",
        'DISCO_TEST_PORT':       "9444",
        'DISCO_ERLANG':          "guess_erlang()",
        'DISCO_HTTPD':           "'lighttpd'",
        'DISCO_WWW_ROOT':        "os.path.join(DISCO_MASTER_HOME, 'www')",
        'PYTHONPATH':            "DISCO_PATH",
        'DISCO_SCHEDULER':       "'fair'",
        'DISCO_SCHEDULER_ALPHA': ".001",
        }

    must_exist = ('DISCO_DATA',
                  'DISCO_ROOT',
                  'DISCO_MASTER_HOME',
                  'DISCO_MASTER_ROOT',
                  'DISCO_LOG_DIR',
                  'DISCO_PID_DIR')

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
        return settings

def guess_erlang():
    if os.uname()[0] == 'Darwin':
        return '/usr/libexec/StartupItemContext erl'
    return 'erl'
