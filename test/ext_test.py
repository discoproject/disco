
import sys
from disco_external import *
from netstring import encode_netstring_fd

params = {"test1": "1,2,3",\
          "one two three": "dim\ndam\n",\
          "dummy": "value"}

open_ext("./ext_test", encode_netstring_fd(params))

for i in range(10):
        e = ("testing %d" % i, "daa: %d" % i)
        r = ext_map(e, None) 
        print r
        if r != [e] * 3:
                print "Result mismatch:", r
                sys.exit(1)

print "looks ok!"



