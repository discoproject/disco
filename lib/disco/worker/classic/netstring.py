
# Copyright (c) 2007 Ville H. Tuulos
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from disco.compat import StringIO

MAX_LEN_STRING = 10
MAX_PACKET_LEN = 1024**3

class NetStringError(Exception):
    pass

def _read_string(msg, i):
    j = msg.index(" ", i)
    length = int(msg[i: j])
    j += 1
    return (j + length + 1, msg[j: j + length])


def encode_netstring_str(d):
    msg = StringIO()
    for k, v in d:
        msg.write("{0} {1} {2} {3}\n"
                  .format(len(k), str(k), len(v), str(v)))
    return msg.getvalue()

def encode_netstring_fd(d):
    s = encode_netstring_str(d.items())
    return "{0}\n{1}".format(len(s), s)

def decode_netstring_str(msg):
    i = 0
    d = []
    while i < len(msg):
        i, key = _read_string(msg, i)
        i, val = _read_string(msg, i)
        d.append((key, val))
    return d

def decode_netstring_fd(fd):
    i = 0
    lenstr = ""
    while 1:
        c = fd.read(1)
        if not c:
            raise EOFError()
        elif c.isspace():
            break
        lenstr += c
        i += 1
        if i > MAX_LEN_STRING:
            raise NetStringError("Length string too long")

    if not lenstr:
        raise EOFError()

    length = int(lenstr)
    if length > MAX_PACKET_LEN:
        raise NetStringError("Will not receive {0} bytes".format(length))

    return dict(decode_netstring_str(fd.read(length)))
