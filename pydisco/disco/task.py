import os, hashlib

from disco.settings import DiscoSettings

class Task(object):
    default_paths = {
        'CHDIR_PATH':    "",
        'PARAMS_FILE':   "params.dl",
        'REQ_FILES':     "lib",
        'EXT_MAP':       "ext.map",
        'EXT_REDUCE':    "ext.reduce",
        'MAP_OUTPUT':    "map-disco-%d-%.9d",
        'PART_OUTPUT':   "part-disco-%.9d",
        'REDUCE_DL':     "reduce-in-%d.dl",
        'REDUCE_SORTED': "reduce-in-%d.sorted",
        'REDUCE_OUTPUT': "reduce-disco-%d",
        'OOB_FILE':      "oob/%s",
        'MAP_INDEX':     "map-index.txt",
        'REDUCE_INDEX':  "reduce-index.txt"
    }

    def __init__(self,
                 mode = None,
                 host = None,
                 master = None,
                 job_name = '',
                 id = -1,
                 inputs = None,
                 result_iterator = False):
        self.id = int(id)
        self.mode = mode
        self.host = host
        self.master = master
        self.inputs = inputs
        self.name = job_name
        self.result_iterator = result_iterator

        settings   = DiscoSettings()
        self.root  = settings['DISCO_ROOT']
        self.port  = settings['DISCO_PORT']
        self.flags = settings['DISCO_FLAGS'].lower().split()

    @property
    def hex_key(self):
        return hashlib.md5(self.name).hexdigest()[:2]

    @property
    def home(self):
        return os.path.join(str(self.host), self.hex_key, self.name)

    @property
    def jobroot(self):
        datadir = 'temp' if self.has_flag('resultfs') else 'data'
        return os.path.join(self.root, datadir, self.home)

    def has_flag(self, flag):
        return flag.lower() in self.flags

    def path(self, name, *args):
        path = self.default_paths[name] % args
        return os.path.join(self.jobroot, path)

    def url(self, name, *args, **kwargs):
        path = self.default_paths[name] % args
        scheme = kwargs.get('scheme', 'disco')
        return '%s://%s/%s/%s' % (scheme, self.host, self.home, path)

    @property
    def map_index(self):
        return (self.path('MAP_INDEX'),
                self.url('MAP_INDEX', scheme='dir'))

    @property
    def reduce_index(self):
        return (self.path('REDUCE_INDEX'),
                self.url('REDUCE_INDEX', scheme='dir'))

    def map_output(self, partition):
        return (self.path('MAP_OUTPUT', self.id, partition),
                self.url('MAP_OUTPUT', self.id, partition))

    def partition_output(self, partition):
        return (self.path('PART_OUTPUT', partition),
                self.url('PART_OUTPUT', partition))

    def reduce_output(self):
        return (self.path('REDUCE_OUTPUT', self.id),
                self.url('REDUCE_OUTPUT', self.id))

    def oob_file(self, key):
        return self.path('OOB_FILE', key)
