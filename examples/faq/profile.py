"""
Try running this from the ``examples/faq/`` directory using:

disco run profile.ProfileJob http://example.com/data | xargs disco wait && xargs disco pstats -k cumulative
"""
from disco.job import Job

class ProfileJob(Job):
    profile = True

    @staticmethod
    def map(entry, params):
        yield entry.strip(), 1
