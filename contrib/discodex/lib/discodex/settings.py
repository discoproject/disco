import os

from clx.settings import Settings
from disco.settings import guess_settings

def guess_home():
    from discodex.core import DiscodexError
    discodex_lib  = os.path.dirname(os.path.realpath(__file__))
    discodex_home = os.path.dirname(os.path.dirname(discodex_lib))
    if os.path.exists(os.path.join(discodex_home, '.discodex-home')):
        return discodex_home
    raise DiscodexError("DISCODEX_HOME is not specified, where should Discodex live?")

class DiscodexSettings(Settings):
    defaults = {
        'DISCODEX_HOME':         "guess_home()",
        'DISCODEX_SETTINGS':     "guess_settings()",
        'DISCODEX_ETC_DIR':      "os.path.join(DISCODEX_HOME, 'etc')",
        'DISCODEX_LOG_DIR':      "os.path.join(DISCODEX_HOME, 'log')",
        'DISCODEX_PID_DIR':      "os.path.join(DISCODEX_HOME, 'run')",
        'DISCODEX_WWW_ROOT':     "os.path.join(DISCODEX_HOME, 'www')",
        'DISCODEX_PURGE_FILE':   "os.path.join(DISCODEX_ETC_DIR, 'purge')",
        'DISCODEX_HTTP_HOST':    "'localhost'",
        'DISCODEX_HTTP_PORT':    "8080",
        'DISCODEX_SCGI_HOST':    "'localhost'",
        'DISCODEX_SCGI_PORT':    "3033",
        'DISCODEX_LIGHTTPD':     "'lighttpd'",
        'DISCODEX_DISCO_MASTER': "'disco://localhost'",
        'DISCODEX_DISCO_PREFIX': "'_discodex'",
        'DISCODEX_INDEX_PREFIX': "'discodex'",
        }

    globals = globals()
    settings_file_var = 'DISCODEX_SETTINGS'
