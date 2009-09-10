import re, cPickle
from disco.util import err, data_err, msg

def netstr_reader(fd, content_len, fname):

        if content_len == None:
                err("Content-length must be defined for netstr_reader")
        def read_netstr(idx, data, tot):
                ldata = len(data)
                i = 0
                lenstr = ""
                if ldata - idx < 11:
                        data = data[idx:] + fd.read(8192)
                        ldata = len(data)
                        idx = 0

                i = data.find(" ", idx, idx + 11)
                if i == -1:
                        err("Corrupted input (%s). Could not "\
                               "parse a value length at %d bytes."\
                                        % (fname, tot))
                else:
                        lenstr = data[idx:i + 1]
                        idx = i + 1

                if ldata < i + 1:
                        data_err("Truncated input (%s). "\
                                "Expected %d bytes, got %d" %\
                                (fname, content_len, tot), fname)
                
                try:
                        llen = int(lenstr)
                except ValueError:
                        err("Corrupted input (%s). Could not "\
                                "parse a value length at %d bytes."\
                                        % (fname, tot))

                tot += len(lenstr)

                if ldata - idx < llen + 1:
                        data = data[idx:] + fd.read(llen + 8193)
                        ldata = len(data)
                        idx = 0

                msg = data[idx:idx + llen]
                
                if idx + llen + 1 > ldata:
                        data_err("Truncated input (%s). "\
                                "Expected a value of %d bytes "\
                                "(offset %u bytes)" %\
                                (fname, llen + 1, tot), fname)

                tot += llen + 1
                idx += llen + 1
                return idx, data, tot, msg
        
        data = fd.read(8192)
        tot = idx = 0
        while tot < content_len:
                key = val = ""
                idx, data, tot, key = read_netstr(idx, data, tot)
                idx, data, tot, val = read_netstr(idx, data, tot)
                yield key, val


def re_reader(item_re_str, fd, content_len, fname, output_tail = False, read_buffer_size=8192):
        item_re = re.compile(item_re_str)
        buf = ""
        tot = 0
        while True:
                try:
                        if content_len:
                                r = fd.read(min(read_buffer_size, content_len - tot))
                        else:
                                r = fd.read(read_buffer_size)
                        tot += len(r)
                        buf += r
                except:
                        data_err("Receiving data failed", fname)

                m = item_re.match(buf)
                while m:
                        yield m.groups()
                        buf = buf[m.end():]
                        m = item_re.match(buf)

                if not len(r) or tot >= content_len:
                        if content_len != None and tot < content_len:
                                data_err("Truncated input (%s). "\
                                         "Expected %d bytes, got %d" %\
                                         (fname, content_len, tot), fname)
                        if len(buf):
                                if output_tail:
                                        yield [buf]
                                else:
                                        msg("Couldn't match the last %d "\
                                            "bytes in %s. Some bytes may be "\
                                            "missing from input." %\
                                                (len(buf), fname))
                        break


def default_partition(key, nr_reduces, params):
        return hash(str(key)) % nr_reduces


def make_range_partition(min_val, max_val):
        r = max_val - min_val
        f = "lambda k, n, p: int(round(float(int(k) - %d) / %d * (n - 1)))" %\
                (min_val, r)
        return eval(f)


def nop_reduce(iter, out, params):
        for k, v in iter:
                out.add(k, v)


def map_line_reader(fd, sze, fname):
        for x in re_reader("(.*?)\n", fd, sze, fname, output_tail = True):
                yield x[0]

def netstr_writer(fd, key, value, params):
        skey = str(key)
        sval = str(value)
        fd.write("%d %s %d %s\n" % (len(skey), skey, len(sval), sval))

def object_writer(fd, key, value, params):
        skey = cPickle.dumps(key, cPickle.HIGHEST_PROTOCOL)
        sval = cPickle.dumps(value, cPickle.HIGHEST_PROTOCOL)
        fd.write("%d %s %d %s\n" % (len(skey), skey, len(sval), sval))

def object_reader(fd, sze, fname):
        for k, v in netstr_reader(fd, sze, fname):
                yield (cPickle.loads(k), cPickle.loads(v))


chain_reader = netstr_reader









                


        


        





        




