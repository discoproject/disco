import os

class DiscodexSettings(dict):
    defaults = {
        'DISCODEX_MODULE':       "os.path.dirname(os.path.realpath(__file__))",
        'DISCODEX_LIB':          "os.path.dirname(DISCODEX_MODULE)",
        'DISCODEX_HOME':         "os.path.dirname(DISCODEX_LIB)",
        'DISCODEX_ETC_DIR':      "os.path.join(DISCODEX_HOME, 'etc')",
        'DISCODEX_LOG_DIR':      "os.path.join(DISCODEX_HOME, 'log')",
        'DISCODEX_PID_DIR':      "os.path.join(DISCODEX_HOME, 'run')",
        'DISCODEX_WWW_ROOT':     "os.path.join(DISCODEX_HOME, 'www')",
        'DISCODEX_SETTINGS':     "os.path.join(DISCODEX_ETC_DIR, 'settings.py')",
        'DISCODEX_PURGE_FILE':   "os.path.join(DISCODEX_ETC_DIR, 'purge')",
        'DISCODEX_DATA_ROOT':    "os.path.join(DISCODEX_WWW_ROOT, 'data')",
        'DISCODEX_INDEX_ROOT':   "os.path.join(DISCODEX_DATA_ROOT, 'indices')",
        'DISCODEX_INDEX_TEMP':   "os.path.join(DISCODEX_DATA_ROOT, 'tmp')",
        'DISCODEX_HTTP_HOST':    "'localhost'",
        'DISCODEX_HTTP_PORT':    "8080",
        'DISCODEX_SCGI_HOST':    "'localhost'",
        'DISCODEX_SCGI_PORT':    "3033",
        'DISCODEX_LIGHTTPD':     "'lighttpd'",
        'DISCODEX_DISCO_MASTER': "'disco://localhost'",
        'DISCODEX_DISCO_PREFIX': "'_discodex'",
        }

    def __init__(self, **kwargs):
        super(DiscodexSettings, self).__init__(kwargs)
        execfile(self['DISCODEX_SETTINGS'], {}, self)

    def __getitem__(self, key):
        if key in os.environ:
            return os.environ[key]
        if key not in self:
            return eval(self.defaults[key], globals(), self)
        return super(DiscodexSettings, self).__getitem__(key)

    def safedir(self, key):
        path = self[key]
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    @property
    def env(self):
        settings = os.environ.copy()
        settings.update(dict((k, str(self[k])) for k in self.defaults))
        return settings
