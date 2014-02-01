import unittest
import disco.fileutils as fileutils
from disco.error import DataError

# value far bigger than today's hdds
fileutils.MIN_DISK_SPACE = 2**64

class FileutilsTest(unittest.TestCase):

    def test_ensure_free_space(self):
        self.assertRaises(DataError, fileutils.ensure_free_space, "/tmp")