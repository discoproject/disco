from random import random

from disco.json import loads
from disco.test import TestCase

def random_bytes(size):
    return ''.join(chr(int(random() * 255)) for x in xrange(size))

class JobPackTestCase(TestCase):
    def test_invalid(self):
        for size in (200, 500, 1000, 10000):
            status, response = loads(self.disco.request('/disco/job/new',
                                                        random_bytes(size)))
            self.assertEquals(status, 'error')
            self.assertLess(len(response), size)

