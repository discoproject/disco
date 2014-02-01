from disco.test import TestCase, TestJob
from disco.error import JobError

class BadJob(TestJob):
    @staticmethod
    def map(e, params):
        print(e)
        return [(e.strip(), '')]


class RateLimitTestCase(TestCase):
    num_lines = 100
    inputs = [1]

    def serve(self, path):
        return b'badger\n' * self.num_lines

    def answers(self):
        return [(l, '') for l in self.serve('').splitlines()]

    def messages(self):
        for offset, (tstamp, host, msg) in self.disco.events(self.job.name):
            yield msg

    def test_bad(self):
        self.job = BadJob().run(input=self.test_server.urls([1]))
        self.assertRaises(JobError, self.job.wait)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')
