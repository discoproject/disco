#!/usr/bin/env python
"""
Module/script for controlling the discodex server.
"""

import optparse, os, subprocess, signal, sys
from itertools import chain

class DiscodexError(Exception):
    pass

class server(object):
    def __init__(self, discodex_settings, host=None, port=None):
        self.discodex_settings = discodex_settings
        self.host = host
        self.port = port

    @property
    def env(self):
        return self.discodex_settings.env

    @property
    def id(self):
        return self.__class__.__name__, self.host, self.port

    @property
    def log_file(self):
        return os.path.join(self.discodex_settings['DISCODEX_LOG_DIR'], '%s-%s_%s.log' % self.id)

    @property
    def pid(self):
        return int(open(self.pid_file).readline().strip())

    @property
    def pid_file(self):
        return os.path.join(self.discodex_settings['DISCODEX_PID_DIR'], '%s-%s_%s.pid' % self.id)

    def restart(self):
        return chain(self.stop(), self.start())

    def send(self, command):
        return getattr(self, command)()

    def start(self, **kwargs):
        if self._status == 'running':
            raise DiscodexError("%s already started" % self)
        process = subprocess.Popen(self.args, env=self.env, **kwargs)
        if process.wait():
            raise DiscodexError("Failed to start %s" % self)
        yield '%s started' % self

    @property
    def _status(self):
        try:
            os.getpgid(self.pid)
            return 'running'
        except Exception:
            return 'stopped'

    def status(self):
        yield '%s %s' % (self, self._status)

    def stop(self):
        try:
            os.kill(self.pid, signal.SIGTERM)
            while self._status == 'running':
                pass
        except Exception:
            pass
        return chain(self.status())

    def __str__(self):
        return '%s %s:%s' % self.id

class lighttpd(server):
    @property
    def args(self):
        return [self.discodex_settings['DISCODEX_LIGHTTPD'], '-f', self.conf]

    @property
    def conf(self):
        return os.path.join(self.discodex_settings['DISCODEX_ETC_DIR'], 'lighttpd.conf')

    @property
    def env(self):
        env = self.discodex_settings.env
        env.update({'DISCODEX_HTTP_LOG': self.log_file,
                    'DISCODEX_HTTP_PID': self.pid_file})
        return env

class djangoscgi(server):
    @property
    def args(self):
        settings = self.discodex_settings
        program  = os.path.join(settings['DISCODEX_WWW_ROOT'], 'manage.py')
        return [program, 'runfcgi', 'protocol=scgi',
                'host=%s' % self.host,
                'port=%s' % self.port,
                'pidfile=%s' % self.pid_file,
                'outlog=%s' % self.log_file,
                'errlog=%s' % self.log_file]


    @property
    def lighttpd(self):
        return lighttpd(self.discodex_settings,
                        self.discodex_settings['DISCODEX_HTTP_HOST'],
                        self.discodex_settings['DISCODEX_HTTP_PORT'])

    def send(self, command):
        return chain(getattr(self, command)(), self.lighttpd.send(command))


def main():
    DISCODEX_BIN  = os.path.realpath(os.path.dirname(__file__))
    DISCODEX_HOME = os.path.dirname(DISCODEX_BIN)

    option_parser = optparse.OptionParser()
    option_parser.add_option('-s', '--settings',
                             help='use settings file settings')
    option_parser.add_option('-v', '--verbose',
                             action='store_true',
                             help='print debugging messages')
    option_parser.add_option('-p', '--print-env',
                             action='store_true',
                             help='print the parsed environment and exit')
    options, sys.argv = option_parser.parse_args()

    if options.settings:
        os.setenv('DISCODEX_SETTINGS', options.settings)

    sys.path.insert(0, os.path.join(DISCODEX_HOME, 'lib'))
    from discodex.settings import DiscodexSettings
    discodex_settings = DiscodexSettings()

    if options.verbose:
        print(
            """
            It seems that Discodex is at {DISCODEX_HOME}
            Disco settings are at {0}

            If this is not what you want, see the `--help` option
            """.format(options.settings, **discodex_settings))

    if options.print_env:
        for item in sorted(discodex_settings.env.iteritems()):
            print('%s = %s' % (item))
        sys.exit(0)

    discodex_server = djangoscgi(discodex_settings,
                                 host=discodex_settings['DISCODEX_SCGI_HOST'],
                                 port=discodex_settings['DISCODEX_SCGI_PORT'])

    argdict = dict(enumerate(sys.argv))
    for message in discodex_server.send(argdict.pop(0, 'status')):
        print(message)

if __name__ == '__main__':
    try:
        main()
    except DiscodexError, e:
        sys.exit(e)
    except Exception, e:
        print('Discodex encountered an unexpected error:')
        sys.exit(e)
