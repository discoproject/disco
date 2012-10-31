import cStringIO
import os

from disco import comm
from boto.s3.connection import S3Connection
from boto.s3.key import Key

def open(url):
    from disco.settings import DiscoSettings
    settings = DiscoSettings()
  
    key = os.path.basename(url)
    (bucket, dirname) = os.path.split(os.path.dirname(url)[5:])
    conn = S3Connection(settings['AWS_ACCESS_KEY_ID'], settings['AWS_SECRET_KEY'])
    bucket = conn.create_bucket(bucket)
    k = Key(bucket)
    k.key = os.path.join(dirname, key)
    return k

def input_stream(fd, sze, url, params):
    """Opens the specified url using an http client."""
    k = open(url)
    contents = k.get_contents_as_string()
    return cStringIO.StringIO(contents), len(contents), url
