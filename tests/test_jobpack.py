from random import random

from disco.json import loads
from disco.job import JobPack
from disco.test import TestCase
from disco.error import JobError

def random_bytes(size):
    return ''.join(chr(int(random() * 255)) for x in xrange(size))

hdr_size = JobPack.HEADER_SIZE
class JobPackTestCase(TestCase):
    def test_badmagic(self):
        offsets = [hdr_size, hdr_size + 1, hdr_size + 2, hdr_size + 3]
        jobpack = JobPack.header(offsets, magic=0) + '0'*4
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

    def test_badheader(self):
        offsets = [hdr_size + 1, hdr_size + 2, hdr_size + 2, hdr_size + 2]
        jobpack = JobPack.header(offsets) + '0'*3
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

        offsets = [hdr_size, hdr_size, hdr_size + 1, hdr_size + 1]
        jobpack = JobPack.header(offsets) + '0'*2
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

        offsets = [hdr_size, hdr_size+1, hdr_size, hdr_size + 1]
        jobpack = JobPack.header(offsets) + '0'*2
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

        offsets = [hdr_size, hdr_size+1, hdr_size+1, hdr_size]
        jobpack = JobPack.header(offsets) + '0'*2
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

    def test_baddict(self):
        offsets = [hdr_size, hdr_size+1, hdr_size+1, hdr_size+1]
        jobpack = JobPack.header(offsets) + '0'*2
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')

class JobPackInfoTestCase(TestCase):
    def test_badinfo(self):
        jobenvs, jobzip, jobdata = {}, '', ''
        jobdict = {}
        jobpack = JobPack(jobdict, jobenvs, jobzip, jobdata)
        status, response = loads(self.disco.request('/disco/job/new', jobpack.dumps()))
        self.assertEquals(status, 'error')
        self.assertTrue(response.find("missing key") >= 0)

    def test_badprefix(self):
        jobenvs, jobzip, jobdata = {}, '', ''
        jobdict = {'prefix':'a/b', 'scheduler':{}, 'input':[],
                   'worker':"w", 'owner':"o", 'nr_reduces':"2"}
        jobpack = JobPack(jobdict, jobenvs, jobzip, jobdata)
        status, response = loads(self.disco.request('/disco/job/new', jobpack.dumps()))
        self.assertEquals(status, 'error')
        self.assertTrue(response.find("invalid prefix") >= 0)

        jobdict = {'prefix':'a.b', 'scheduler':{}, 'input':[],
                   'worker':"w", 'owner':"o", 'nr_reduces':"2"}
        jobpack = JobPack(jobdict, jobenvs, jobzip, jobdata)
        status, response = loads(self.disco.request('/disco/job/new', jobpack.dumps()))
        self.assertEquals(status, 'error')
        self.assertTrue(response.find("invalid prefix") >= 0)

class JobPackLengthTestCase(TestCase):
    def test_badlength(self):
        jobenvs, jobzip, jobdata = {}, '0'*64, '0'*64
        jobdict = {'prefix':'JobPackBadLength', 'scheduler':{}, 'input':["raw://data"],
                   "map?":True, 'worker':"w", 'owner':"o", 'nr_reduces':"2"}
        jobpack = JobPack(jobdict, jobenvs, jobzip, jobdata).dumps()
        jobpack = jobpack[:(len(jobpack)-len(jobdata)-1)]
        status, response = loads(self.disco.request('/disco/job/new', jobpack))
        self.assertEquals(status, 'error')
        self.assertTrue(response.find("invalid_header") >= 0)

    # Zip extraction failures are currently treated as non-fatal errors.
    #
    # def test_badzip(self):
    #     jobenvs, jobzip, jobdata = {}, '0'*64, '0'*64
    #     jobdict = {'prefix':'JobPackBadZip', 'scheduler':{}, 'input':["raw://data"],
    #                "map?":True, 'worker':"w", 'owner':"o", 'nr_reduces':"2"}
    #     jobpack = JobPack(jobdict, jobenvs, jobzip, jobdata).dumps()
    #     status, jobname = loads(self.disco.request('/disco/job/new', jobpack))
    #     self.assertEquals(status, 'ok')
    #     self.assertRaises(JobError, lambda: self.disco.wait(jobname))
