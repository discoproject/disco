import os

from clx.server import Server

class DiscodexServer(Server):
    @property
    def log_dir(self):
        return self.settings.safedir('DISCODEX_LOG_DIR')

    @property
    def pid_dir(self):
        return self.settings.safedir('DISCODEX_PID_DIR')

class lighttpd(DiscodexServer):
    @property
    def host(self):
        return self.settings['DISCODEX_HTTP_HOST']

    @property
    def port(self):
        return self.settings['DISCODEX_HTTP_PORT']

    @property
    def args(self):
        return [self.settings['DISCODEX_LIGHTTPD'], '-f', self.conf]

    @property
    def conf(self):
        return os.path.join(self.settings['DISCODEX_ETC_DIR'], 'lighttpd.conf')

    @property
    def env(self):
        env = self.settings.env
        env.update({'DISCODEX_HTTP_LOG': self.log_file,
                    'DISCODEX_HTTP_PID': self.pid_file})
        return env

class djangoscgi(DiscodexServer):
    @property
    def host(self):
        return self.settings['DISCODEX_SCGI_HOST']

    @property
    def port(self):
        return self.settings['DISCODEX_SCGI_PORT']

    @property
    def args(self):
        settings = self.settings
        program  = os.path.join(settings['DISCODEX_WWW_ROOT'], 'manage.py')
        return [program, 'runfcgi', 'protocol=scgi',
                'host=%s' % self.host,
                'port=%s' % self.port,
                'pidfile=%s' % self.pid_file,
                'outlog=%s' % self.log_file,
                'errlog=%s' % self.log_file]
