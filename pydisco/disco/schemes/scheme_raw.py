import cStringIO

def input_stream(fd, sze, url, params):
        return (cStringIO.StringIO(url[6:]), len(url) - 6, url)
        
