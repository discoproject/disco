import os, signal, subprocess
from itertools import chain
from logging.handlers import TimedRotatingFileHandler

class ServerError(Exception):
    pass

class Server(object):
    """
    Server is an abstract base class.

    Concrete implementations must provide `host`, `port`, `log_dir`, `pid_dir`.
    Some subclasses may also need to provide `env` if the default is not satisfactory.
    """
    def __init__(self, settings, rotate_log=False):
        self.settings = settings
        self.rotate_log = rotate_log

    @property
    def env(self):
        return self.settings.env

    @property
    def id(self):
        return self.__class__.__name__, self.host, self.port

    @property
    def log_file(self):
        return os.path.join(self.log_dir, '{0[0]}-{0[1]}_{0[2]}.log'.format(self.id))

    def log_rotate(self):
        TimedRotatingFileHandler(self.log_file, when='S', interval=1).doRollover()

    @property
    def pid(self):
        return int(open(self.pid_file).readline().strip())

    @property
    def pid_file(self):
        return os.path.join(self.pid_dir, '{0[0]}-{0[1]}_{0[2]}.pid'.format(self.id))

    def restart(self):
        return chain(self.stop(), self.start())

    def start(self, *args, **kwargs):
        if self._status == 'running':
            raise ServerError("{0} already started".format(self))
        if self.rotate_log:
            self.log_rotate()
        process = subprocess.Popen(args or self.args, env=self.env, **kwargs)
        if process.wait():
            raise ServerError("Failed to start {0}".format(self))
        yield '{0} started'.format(self)

    @property
    def _status(self):
        try:
            os.getpgid(self.pid)
            return 'running'
        except Exception:
            return 'stopped'

    def status(self):
        yield '{0} {1}'.format(self, self._status)

    def stop(self):
        try:
            os.kill(self.pid, signal.SIGTERM)
            while self._status == 'running':
                pass
        except Exception:
            pass
        return chain(self.status())

    def __str__(self):
        return '{0[0]} {0[1]}:{0[2]}'.format(self.id)
