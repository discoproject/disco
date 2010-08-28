from disco.test import DiscoJobTestFixture, DiscoMultiJobTestFixture
from disco.test import DiscoTestCase
from disco.func import chain_reader
from disco.util import ddfs_name
from disco.ddfs import DDFS

class SaveOnlyMapTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = map(str, range(10))
    partitions = None
    save = True

    def getdata(self, path):
        return '%s\n' % path

    @staticmethod
    def map(e, params):
        return [(e + "#", '')]

    @property
    def answers(self):
        return [('%s#' % e, '') for e in self.inputs]

    def runTest(self):
        results = sorted(list(self.results))
        ddfs = DDFS(self.disco_master_url)
        tag = self.disco.results(self.job.name)[1][0]
        self.assertEquals(len(list(ddfs.blobs(tag))), len(self.inputs))
        self.assertEquals(self.answers, results)

    def tearDown(self):
        super(SaveOnlyMapTestCase, self).tearDown()
        DDFS(self.disco_master_url).delete(ddfs_name(self.job.name))

class SaveTestCase(DiscoMultiJobTestFixture, DiscoTestCase):
    njobs = 2
    inputs_1 = ['huey', 'dewey', 'louie']
    partitions_1 = 3
    partitions_2 = 1
    map_reader_2 = chain_reader
    save_1 = True
    save_2 = True
    sort_2 = True

    def getdata(self, path):
        return path + "\n"

    @property
    def input_2(self):
        return self.job_1.wait()

    @staticmethod
    def map_1(e, params):
        if type(e) == tuple:
            e = e[0]
        yield (e + "!", '')

    map_2 = map_1

    @staticmethod
    def reduce_1(iter, out, params):
        for k, v in iter:
            out.add(k + "?", v)

    reduce_2 = reduce_1

    @property
    def answers(self):
        return [('dewey!?!?', ''), ('huey!?!?', ''), ('louie!?!?', '')]

    def runTest(self):
        self.assertEquals(self.answers, list(self.results_2))

    def tearDown(self):
        super(SaveTestCase, self).tearDown()
        DDFS(self.disco_master_url).delete(ddfs_name(self.job_1.name))
        DDFS(self.disco_master_url).delete(ddfs_name(self.job_2.name))






