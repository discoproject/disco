#!/usr/bin/env python

import optparse, os, subprocess, signal, sys
from itertools import chain

class DiscoSettings(dict):
    defaults = {
        'DISCO_FLAGS':          "''",
        'DISCO_LOG_DIR':        "'/var/log/disco'",
        'DISCO_MASTER_HOME':    "'/usr/lib/disco'",
        'DISCO_MASTER_PORT':    "DISCO_PORT",
        'DISCO_NAME':           "'disco_%s' % DISCO_SCGI_PORT",
        'DISCO_PID_DIR':        "'/var/run'",
        'DISCO_PORT':           "8989",
        'DISCO_ROOT':           "'/srv/disco'",
        'DISCO_SCGI_PORT':      "4444",
        'DISCO_ULIMIT':         "16000000",
        'DISCO_USER':           "os.getlogin()",
        'DISCO_DATA':           "os.path.join(DISCO_ROOT, 'data')",
        'DISCO_MASTER_ROOT':    "os.path.join(DISCO_DATA, '_%s' % DISCO_NAME)",
        'DISCO_CONFIG':         "os.path.join(DISCO_ROOT, '%s.config' % DISCO_NAME)",
        'DISCO_MASTER_LOG':     "os.path.join(DISCO_LOG_DIR, '%s.log' % DISCO_NAME)",
        'DISCO_MASTER_PID':     "os.path.join(DISCO_PID_DIR, 'disco-master.pid')",
        'DISCO_LOCAL_DIR':      "os.path.join(DISCO_ROOT, 'local', '_%s' % DISCO_NAME)",
        'DISCO_WORKER':         "os.path.join(DISCO_HOME, 'node', 'disco-worker')",
        'ERLANG':               "guess_erlang()",
        'LIGHTTPD':             "'lighttpd'",
        'LIGHTTPD_MASTER_ROOT': "os.path.join(DISCO_MASTER_HOME, 'www')",
        # only need this if disco isn't in site-packages
        'PYTHONPATH':           "'%s:%s/pydisco' % (os.getenv('PYTHONPATH', ''), DISCO_HOME)",
        }

    must_exist = ('DISCO_DATA', 'DISCO_MASTER_ROOT')

    def __init__(self, filename, **kwargs):
        super(DiscoSettings, self).__init__(kwargs)
        execfile(filename, {}, self)

    def __getitem__(self, key):
        if key not in self:
            return eval(self.defaults[key], globals(), self)
        return super(DiscoSettings, self).__getitem__(key)

    @property
    def env(self):
        settings = os.environ.copy()
        settings.update(dict((k, str(self[k])) for k in self.defaults))
        return settings

class DiscoError(Exception):
    pass

class server(object):
    def __init__(self, disco_settings):
        self.disco_settings = disco_settings

    @property
    def env(self):
        return self.disco_settings.env

    @property
    def pid(self):
        return int(open(self.pid_file).readline().strip())

    def conf_path(self, filename):
        return os.path.join(self.disco_settings['DISCO_CONF'], filename)

    def restart(self):
        return chain(self.stop(), self.start())

    def send(self, command):
        return getattr(self, command)()

    def start(self, **kwargs):
        if self._status == 'running':
            raise DiscoError("%s already started" % self)
        process = subprocess.Popen(self.args, env=self.env, **kwargs)
        if process.wait():
            raise DiscoError("Failed to start %s" % self)
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
        return ' '.join(self.args)

class lighttpd(server):
    def __init__(self, disco_settings, config_file, port):
        super(lighttpd, self).__init__(disco_settings)
        self.config_file = config_file
        self.port = port

    @property
    def args(self):
        return [self.disco_settings['LIGHTTPD'], '-f', self.config_file]

    @property
    def env(self):
        env = self.disco_settings.env
        env.update({'LIGHTTPD_LOG': self.log_file,
                    'LIGHTTPD_PID': self.pid_file,
                    'LIGHTTPD_PORT': str(self.port)})
        return env

    @property
    def log_file(self):
        # could change to be unique to host:port instead of just port
        return os.path.join(self.disco_settings['DISCO_LOG_DIR'], 'lighttpd-%s.log' % self.port)
        
    @property
    def pid_file(self):
        # could change to be unique to host:port instead of just port
        return os.path.join(self.disco_settings['DISCO_PID_DIR'], 'lighttpd-%s.pid' % self.port)
        
class master(server):
    @property
    def args(self):
        settings = self.disco_settings
        return settings['ERLANG'].split() + \
               ['+K', 'true',
                '-heart',
                '-detached',
                '-sname', '%s_master' % settings['DISCO_NAME'],
                '-rsh', 'ssh',
                '-connect_all', 'false',
                '-pa', os.path.join(settings['DISCO_MASTER_HOME'], 'ebin'),
                '-kernel', 'error_logger', '{file, "%s"}' % settings['DISCO_MASTER_LOG'],
                '-disco', 'disco_name', '"%s"' % settings['DISCO_NAME'],
                '-disco', 'disco_root', '"%s"' % settings['DISCO_MASTER_ROOT'],
                '-disco', 'scgi_port', '%s' % settings['DISCO_SCGI_PORT'],
                '-disco', 'disco_localdir', '"%s"' % settings['DISCO_LOCAL_DIR'],
                '-eval', '[handle_job, handle_ctrl]',
                '-eval', 'application:start(disco)']
    
    @property
    def lighttpd(self):
        return lighttpd(self.disco_settings,
                        self.conf_path('lighttpd-master.conf'),
                        self.disco_settings['DISCO_MASTER_PORT'])
    @property
    def pid_file(self):
        return self.disco_settings['DISCO_MASTER_PID']

    def send(self, command):
        return chain(getattr(self, command)(), self.lighttpd.send(command))
    
    def __str__(self):
        return 'disco master'

class worker(server):
    @property
    def lighttpd(self):
        return lighttpd(self.disco_settings,
                        self.conf_path('lighttpd-worker.conf'),
                        self.disco_settings['DISCO_PORT'])        

    def send(self, command):
        return self.lighttpd.send(command)

def guess_erlang():
    if os.uname()[0] == 'Darwin':
        return '/usr/libexec/StartupItemContext erl'
    return 'erl'
    
def main():
    DISCO_BIN  = os.path.realpath(os.path.dirname(__file__))
    DISCO_HOME = os.path.dirname(DISCO_BIN)
    DISCO_CONF = os.path.join(DISCO_HOME, 'conf')

    option_parser = optparse.OptionParser()
    option_parser.add_option('-s', '--settings',
                             default=os.path.join(DISCO_CONF, 'settings.py'),
                             help='use settings file settings')
    option_parser.add_option('-v', '--verbose',
                             action='store_true',
                             help='print debugging messages')
    option_parser.add_option('-p', '--print-env',
                             action='store_true',
                             help='print the parsed disco environment and exit')
    options, sys.argv = option_parser.parse_args()

    disco_settings = DiscoSettings(options.settings,
                                   DISCO_BIN=DISCO_BIN,
                                   DISCO_HOME=DISCO_HOME,
                                   DISCO_CONF=DISCO_CONF)

    if options.verbose:
        print(
            """
            It seems that Disco is at {DISCO_HOME}
            Disco settings are at {0}
            
            If this is not what you want, see the `--help` option
            """.format(options.settings, **disco_settings)) # python2.6+

    for name in disco_settings.must_exist:
        path = disco_settings[name]
        if not os.path.exists(path):
            os.makedirs(path)

    if options.print_env:
        for item in sorted(disco_settings.env.iteritems()):
            print('%s = %s' % (item))
        sys.exit(0)
            
    argdict      = dict(enumerate(sys.argv))
    disco_object = globals()[argdict.pop(0, 'master')](disco_settings)

    for message in disco_object.send(argdict.pop(1, 'status')):
        print(message)

if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        sys.exit(e)
