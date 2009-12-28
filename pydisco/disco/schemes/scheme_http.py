from disco import comm

def input_stream(fd, sze, url, params):
        return comm.open_remote(url)
