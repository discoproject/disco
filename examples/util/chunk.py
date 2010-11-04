"""
An example using Job classes to implement chunking.

Could be run as follows (assuming this module can be found on sys.path):

disco run --param tag chunks:tag chunk.LineChunker /path/to/textfile

or

disco run --param tag chunks:tag chunk.LineChunker existing-tag-containing-textfiles
"""
from disco.core import Job, Params

class LineChunker(Job):
    params = Params(ddfs_master=None, tag=None)

    def _map_input_stream(fd, size, url, params):
        from disco.ddfs import DDFS
        tag = params.tag or 'disco:chunks:%s' % Task.jobname
        master = params.ddfs_master or Task.master
        yield url, DDFS(master).chunk(tag, [url])
    map_input_stream = [_map_input_stream]

    def map(entry, params):
        yield entry

class GzipLineChunker(LineChunker):
    def _map_input_stream(fd, size, url, params):
        from disco.ddfs import DDFS
        from disco.func import gzip_line_reader
        tag = params.tag or 'disco:chunks:%s' % Task.jobname
        master = params.ddfs_master or Task.master
        yield url, DDFS(master).chunk(tag, [url], reader=gzip_line_reader)
    map_input_stream = [_map_input_stream]
