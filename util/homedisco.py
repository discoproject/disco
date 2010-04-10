from __future__ import with_statement
import os
import sys
import cStringIO

from contextlib import contextmanager

from disco.core import Job
from disco.core import result_iterator
from disco.core import util
from disco.node import worker
from disco.netstring import decode_netstring_fd
from disco.settings import DiscoSettings


@contextmanager
def redirected_stderr(new_stderr):
    old_stderr = sys.stderr
    sys.stderr = new_stderr
    try:
        yield None
    finally:
        sys.stderr = old_stderr

def parse_dir(dir_url, partid=None):
    def parse_index(index):
        return [url for id, url in (line.split() for line in index)
                if partid is None or partid == int(id)]
    settings = DiscoSettings()
    scheme, netloc, path = util.urlsplit(dir_url)
    path = '%s/data/%s' % (settings['DISCO_ROOT'], path)
    return parse_index(open(path))


class MsgStream(object):

    def __init__(self):
        self.out = None

    def write(self, msg):
        if msg.startswith('**<OUT>'):
            addr = msg.split()[-2]
            self.out = parse_dir(addr)
        print msg,

class DummyDisco(object):

    def __init__(self):
        self.request_data = None

    def request(self, url, data=None, **kwargs):
        self.request_data = req = decode_netstring_fd(cStringIO.StringIO(data))
        return 'job started:%s' % req['prefix']


class HomeDisco(object):

    def __init__(self, mode, partition='0'):
        self.mode = mode
        self.partition = partition

    def new_job(self, *args, **kwargs):
        master = DummyDisco()
        job = Job(master, **kwargs)
        request = master.request_data

        worker.init(mode=self.mode,
                    host='localhost',
                    master='nohost',
                    job_name=job.name,
                    id=1,
                    inputs=kwargs.get('input'))

        out = MsgStream()
        with redirected_stderr(out):
            if self.mode == 'map':
                worker.op_map(request)
            elif self.mode == 'reduce':
                worker.op_reduce(request)
            else:
                raise ValueError(
                    "Unknown mode: %s (must be 'map' or 'reduce')" % self.mode)

        return out.out


if __name__ == '__main__':

    os.environ['DISCO_ROOT'] = os.getcwd()

    def fun_map(e, params):
        return [(e, e)]

    def fun_reduce(iter, out, params):
        for k, v in iter:
            out.add('red:' + k, v)


    with open('homedisco-test', 'w') as fout:
        fout.write('dog\ncat\npossum')

    map_hd = HomeDisco('map')
    reduce_hd = HomeDisco('reduce')

    res = map_hd.new_job(name='homedisco',
                         input=[os.path.join(os.getcwd(), 'homedisco-test')],
                         map=fun_map,
                         reduce=fun_reduce)

    res = reduce_hd.new_job(name='homedisco',
                            input=res,
                            map=fun_map,
                            reduce=fun_reduce)

    for k, v in result_iterator(res):
        print 'KEY', k, 'VALUE', v

