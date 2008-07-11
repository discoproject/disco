
import sys
from discoapi import Disco

OK_STATUS = ['job_ready', 'job_died']

disco = Disco(sys.argv[1])

def op_show(n):
        print n

def op_clean(n):
        print "Cleaning", n
        disco.clean(n)

for t, s, name in disco.joblist():
        if s not in OK_STATUS:
                continue
        if sys.argv[3] in name:
                globals()["op_" + sys.argv[2]](name)
