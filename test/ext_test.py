
import sys
from disco_external import *
from netstring import encode_netstring_fd

params = {"test1": "1,2,3",\
          "one two three": "dim\ndam\n",\
          "dummy": "value"}

data = [("testing %d" % i, "daa: %d" % i) for i in range(10)]

class RedOut:
        def add(self, k, v):
                if (k, v) not in data:
                        print "Result mismatch:", (k, v)
                        sys.exit(1)

open_ext("./ext_test", encode_netstring_fd(params))
for e in data:
        r = ext_map(e, None) 
        print r
        if r != [e] * 3:
                print "Result mismatch:", r
                sys.exit(1)

print "map ok!"

open_ext("./ext_test", encode_netstring_fd(params))
ext_reduce(iter(data), RedOut(), encode_netstring_fd(params))

print "reduce ok!"






