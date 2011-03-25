"""
An example using Job classes to implement grep.

Could be run as follows (assuming this module can be found on sys.path):

disco run grep.Grep --param pattern P [tag_url_or_path] ...
"""
from disco.job import Job
from disco.worker.classic.func import nop_map

class Grep(Job):
    map = staticmethod(nop_map)
    params = {}

    @staticmethod
    def map_reader(fd, size, url, params):
        import re
        if params['pattern']:
            pattern = re.compile(params['pattern'])
            for line in fd:
                if pattern.match(line):
                    yield url, line
