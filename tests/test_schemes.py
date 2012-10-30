from disco.job import JobChain
from disco.test import TestCase, TestJob
from disco.compat import file, bytes_to_str

class SchemesJobA(TestJob):
    @staticmethod
    def map(e, params):
        yield e.strip(), None

class SchemesJobB(TestJob):
    scheduler = {'force_local': True}

    @staticmethod
    def map_reader(fd, size, url, params):
        from disco.worker.classic import func
        assert isinstance(fd, file)
        return func.chain_reader(fd, size, url, params)

    @staticmethod
    def map(k_v, params):
        yield bytes_to_str(k_v[0]), k_v[1]

class SchemesTestCase(TestCase):
    animals = ['horse', 'sheep', 'whale', 'tiger']

    def serve(self, path):
        return '\n'.join(self.animals)

    def test_scheme_disco(self):
        a, b = SchemesJobA(), SchemesJobB()
        self.job = JobChain({a: self.test_server.urls([''] * 10),
                             b: a}).wait()
        for key, value in self.results(b):
            self.assert_(key in self.animals)
            self.assertEquals(value, None)
