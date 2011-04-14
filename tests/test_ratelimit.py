from disco.test import TestCase, TestJob
from disco.error import JobError

class BadJob(TestJob):
    @staticmethod
    def map(e, params):
        import sys, disco.json
        msg = disco.json.dumps(e)
        sys.stderr.write('MSG %d %s\n' % (len(msg), msg))
        return []

class GoodJob(TestJob):
    @staticmethod
    def map(e, params):
        import sys, disco.json
        print e
        return [(e.strip(), '')]


class RateLimitTestCase(TestCase):
    num_lines = 100
    inputs = [1]

    def serve(self, path):
        return 'badger\n' * self.num_lines

    def answers(self):
        return [(l, '') for l in self.serve('').splitlines()]

    def messages(self):
        for offset, (tstamp, host, msg) in self.disco.events(self.job.name):
            yield msg

    def test_bad(self):
        self.job = BadJob().run(input=self.test_server.urls([1]))
        self.assertRaises(JobError, self.job.wait)
        self.assertEquals(self.job.jobinfo()['active'], 'dead')

    def test_good(self):
        self.job = GoodJob().run(input=self.test_server.urls([1]))
        self.assertResults(self.job, self.answers())
        num_messages = sum(1 for msg in self.messages() if 'badger' in msg)
        self.assertEquals(num_messages, self.num_lines)

