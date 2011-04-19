"""
An example using Job classes to implement chunking.

Could be run as follows (assuming this module can be found on sys.path):

disco run -P params chunks:tag chunk.LineChunker /path/to/textfile

or

disco run -P params chunks:tag chunk.LineChunker existing-tag-containing-textfiles
"""
from disco.job import Job

class LineChunker(Job):
    params = None

    def _map_input_stream(fd, size, url, params):
        from disco.ddfs import DDFS
        tag = params or 'disco:chunks:%s' % Task.jobname
        yield url, DDFS(Task.master).chunk(tag, [url])
    map_input_stream = [_map_input_stream]

    @staticmethod
    def map(entry, params):
        yield entry

class GzipLineChunker(LineChunker):
    def _map_input_stream(fd, size, url, params):
        from disco.ddfs import DDFS
        from disco.func import gzip_line_reader
        tag = params or 'disco:chunks:%s' % Task.jobname
        yield urlo, DDFS(Task.master).chunk(tag, [url], reader=gzip_line_reader)
    map_input_stream = [_map_input_stream]
