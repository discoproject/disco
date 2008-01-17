import sys, os
import disco

def fun_map(e):
        return [(w, 1) for w in e.split()]

def fun_reduce(iter, out, job):
        s = {}
        for k, v in iter:
                if k in s:
                        s[k] += int(v)
                else:
                        s[k] = int(v)
        for k, v in s.iteritems():
                out.add(k, v)

if "HTTP" in os.environ:
        disco.job(sys.argv[1], "wordcount", 
                sys.argv[2:], fun_map, reduce = fun_reduce)
else:
        disco.job("stdout:", "wordcount",\
                sys.argv[1:], fun_map, reduce = fun_reduce)
