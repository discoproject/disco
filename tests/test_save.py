from disco.ddfs import DDFS
from disco.job import JobChain
from disco.test import TestCase, TestJob
from disco.worker.classic.func import chain_reader

class SaveMapJob(TestJob):
    partitions = None
    save = True

    @staticmethod
    def map(e, params):
        yield e.strip() + '!', ''

class SaveJob1(SaveMapJob):
    partitions = 3
    save = False

    @staticmethod
    def reduce(iter, params):
        for k, v in iter:
            yield k + '?', v

class SaveJob2(SaveJob1):
    partitions = 1
    save = True
    sort = True
    map_reader = staticmethod(chain_reader)

    @staticmethod
    def map((k, v), params):
        yield k + '!', ''

class SaveTestCase(TestCase):
    def serve(self, path):
        return '%s\n' % path

    def test_save_map(self):
        input = range(10)
        self.job = SaveMapJob().run(input=self.test_server.urls(input))
        results = sorted(self.results(self.job))
        self.tag = self.disco.results(self.job.name)[1][0]
        self.assertEquals(len(list(self.ddfs.blobs(self.tag))), len(input))
        self.assertEquals(results, [('%s!' % e, '') for e in input])

    def test_save(self):
        ducks = ['dewey', 'huey', 'louie']
        a, b = SaveJob1(), SaveJob2()
        self.job = JobChain({a: self.test_server.urls(ducks),
                             b: a}).wait()
        self.tag = self.disco.results(b)[1][0]
        self.assertAllEqual(sorted(self.results(b)),
                            [('%s!?!?' % d, '') for d in ducks])

    def tearDown(self):
        super(SaveTestCase, self).tearDown()
        if hasattr(self, 'tag'):
            self.ddfs.delete(self.tag)
