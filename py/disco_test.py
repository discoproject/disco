import sys, os
import disco

def fun_map(e, params):
        return [(w, 1) for w in re.sub("\W", " ", e).lower().split()]

def fun_reduce(iter, out, params):
        s = {}
        for k, v in iter:
                if k in s:
                        s[k] += int(v)
                else:
                        s[k] = int(v)
        for k, v in s.iteritems():
                out.add(k, v)

results = disco.job(sys.argv[1], "wordcount", sys.argv[2:], fun_map, 
		reduce = fun_reduce, 
		nr_reduces = 32,
		sort = False)

for key, value in disco.result_iterator(results):
	print key, value


