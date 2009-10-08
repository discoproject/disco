import os, signal, subprocess
from itertools import chain

from core import DiscodexError

class _server(object):
    def __init__(self, discodex_settings):
        self.discodex_settings = discodex_settings

    @property
    def env(self):
        return self.discodex_settings.env

    @property
    def id(self):
        return self.__class__.__name__, self.host, self.port

    @property
    def log_file(self):
        return os.path.join(self.discodex_settings.safedir('DISCODEX_LOG_DIR'), '%s-%s_%s.log' % self.id)

    @property
    def pid(self):
        return int(open(self.pid_file).readline().strip())

    @property
    def pid_file(self):
        return os.path.join(self.discodex_settings.safedir('DISCODEX_PID_DIR'), '%s-%s_%s.pid' % self.id)

    def restart(self):
        return chain(self.stop(), self.start())

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

    def send(self, command):
        return getattr(self, command)()

    def __str__(self):
        return '%s %s:%s' % self.id

class lighttpd(_server):
    @property
    def host(self):
        return self.discodex_settings['DISCODEX_HTTP_HOST']

    @property
    def port(self):
        return self.discodex_settings['DISCODEX_HTTP_PORT']

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

class djangoscgi(_server):
    @property
    def host(self):
        return self.discodex_settings['DISCODEX_SCGI_HOST']

    @property
    def port(self):
        return self.discodex_settings['DISCODEX_SCGI_PORT']

    @property
    def args(self):
        settings = self.discodex_settings
        program  = os.path.join(settings['DISCODEX_WWW_ROOT'], 'manage.py')
        return [program, 'runfcgi', 'protocol=scgi',
                '--pythonpath=%s' % settings['DISCODEX_LIB'],
                'host=%s' % self.host,
                'port=%s' % self.port,
                'pidfile=%s' % self.pid_file,
                'outlog=%s' % self.log_file,
                'errlog=%s' % self.log_file]

    @property
    def lighttpd(self):
        return lighttpd(self.discodex_settings)

    def send(self, command):
        return chain(getattr(self, command)(), self.lighttpd.send(command))
