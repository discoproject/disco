"""
:mod:`discodex.settings` -- Discodex Settings
=============================================

Discodex settings can be specified either as environment variables or in a file.
The settings use the same mechanism as Disco, and can even be stored in the same file.
For more information on disco settings, see :mod:`disco.settings`.

The most important Discodex settings are:

        *DISCODEX_HOME*
                The directory which Discodex runs out of.
                If you install :mod:`discodex <discodexcli>` using a symlink
                (recommended), you shouldn't need to change this.

        *DISCODEX_PURGE_FILE*
                The path of a file whose existence determines whether or not
                Discodex should purge the results of queries after they have
                been executed.
                Default is ``os.path.join(DISCODEX_HOME, 'etc', 'purge')``.

        *DISCODEX_HTTP_HOST*
                The hostname that the Discodex server should bind to.
                Default is ``localhost``.

        *DISCODEX_HTTP_PORT*
                The port that the Discodex server should bind to.
                Default is ``8080``.

        *DISCODEX_DISCO_MASTER*
                The url of the Disco master that Discodex should submit jobs to.
                Default is ``disco://localhost``.
"""
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
