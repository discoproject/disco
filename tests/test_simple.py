from disco.test import TestCase, TestJob

class SimpleJob(TestJob):
    @staticmethod
    def map(e, params):
        yield int(e), e.strip()

    @staticmethod
    def reduce(iter, out, params):
        for k, v in sorted(iter):
            out.add(k, v)

class SimplerJob(SimpleJob):
    @staticmethod
    def reduce(iter, params):
        return sorted(iter)

class SimpleTestCase(TestCase):
    input = [3, 5, 7, 11, 13, 17, 19, 23, 29, 31]

    def answers(self):
        return ((i, str(i)) for i in self.input for x in xrange(10))

    def serve(self, path):
        return '\n'.join([path] * 10)

    def test_simple(self):
        self.job = SimpleJob().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())

    def test_simpler(self):
        self.job = SimplerJob().run(input=self.test_server.urls(self.input))
        self.assertResults(self.job, self.answers())
