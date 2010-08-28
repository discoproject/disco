"""
An example using Job classes to implement grep.

Could be run as follows (assuming this module can be found on sys.path):

disco run grep.Grep --param pattern P [tag_url_or_path] ...
"""
from disco.core import Job, Params
from disco.func import nop_map

class Grep(Job):
    map = nop_map
    params = Params(pattern=None)

    def map_reader(fd, size, url, params):
        import re
        if params.pattern:
            pattern = re.compile(params.pattern)
            for line in fd:
                if pattern.match(line):
                    yield url, line
