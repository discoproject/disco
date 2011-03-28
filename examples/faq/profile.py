"""
Try running this from the ``examples/faq/`` directory using:

$ disco run profile.ProfileJob http://example.com/data
<JOBNAME>
$ disco pstats -k cumulative <JOBNAME>
"""
from disco.job import Job

class ProfileJob(Job):
    profile = True

    @staticmethod
    def map(entry, params):
        yield entry.strip(), 1
