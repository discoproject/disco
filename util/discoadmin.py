
import sys
from disco.core import Disco

OK_STATUS = ['job_ready', 'job_died']

disco = Disco(sys.argv[1])

def op_show(n, s):
    print n

def op_kill(n, s):
    if s == "job_active":
        print "Killing", n
        disco.kill(n)

def op_clean(n, s):
    print "Cleaning", n
    disco.clean(n)

def op_purge(n, s):
    print "Purging", n
    disco.purge(n)

for t, s, name in disco.joblist():
    if sys.argv[3] in name:
        globals()["op_" + sys.argv[2]](name, s)
