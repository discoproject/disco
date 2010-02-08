
import sys
from disco.node import external
from disco.netstring import encode_netstring_fd

# An ugly workaround for the problem that prevents saying
# "from disco.node import external in external.py.
external.external = external

params = {"test1": "1,2,3",\
          "one two three": "dim\ndam\n",\
          "dummy": "value"}

data = [("testing %d" % i, "daa: %d" % i) for i in range(10)]

class RedOut:
        def add(self, k, v):
                if (k, v) not in data:
                        print "Result mismatch:", (k, v)
                        sys.exit(1)

external.open_ext("./ext_test", encode_netstring_fd(params))
for e in data:
        r = external.ext_map(e, None) 
        print r
        if r != [e] * 3:
                print "Result mismatch:", r
                sys.exit(1)

print "map ok!"

external.open_ext("./ext_test", encode_netstring_fd(params))
external.ext_reduce(iter(data), RedOut(), encode_netstring_fd(params))

print "reduce ok!"






