from disco import comm
from disco.util import inputlist, shuffled, schemesplit

def open(url, task=None):
    _hdfs_scheme, address = schemesplit(url)
    namenode_port, rest = schemesplit(address)
    http_url = 'http://' + namenode_port + '/webhdfs/v1/' + rest + '?op=OPEN'
    return comm.open_url(http_url)

def input_stream(fd, size, url, params):
    file = open(url, task=globals().get('Task'))
    return file, len(file), file.url
