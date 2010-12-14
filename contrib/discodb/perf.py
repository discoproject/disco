#!/usr/bin/env python

from discodb import DiscoDB
from random import choice
from string import letters
from timeit import timeit

def test_leak():
    while True:
        d = DiscoDB(zip(letters, ['abc'] * 1000))
        t = len(d.query('a'))
        t = len(d['b'])
        t = 'd' in d
        t = d.dumps()
        t = DiscoDB.loads(t)
        t = d.dump(open('/tmp/discodb', 'w'))
        t = DiscoDB.load(open('/tmp/discodb'))
        for k in d.keys():
            for v in d.values():
                t = k == v

def len_all(d):
    for k in d.keys():
        len(d.query(k))

def sum_all(d):
    for k in d.keys():
        sum(1 for v in d.query(k))

def create_db(numvs, vsize, disable_compression):
    from itertools import islice, izip, permutations, repeat
    pool = letters * (vsize / len(letters) + 1)
    return DiscoDB(izip(letters, repeat([''.join(p)
                                         for p in islice(permutations(pool, vsize), numvs)])),
                   disable_compression=disable_compression)

def time_db(numvs=10000, vsize=1024, number=100):
    def timer(fn_name, disable_compression):
        return timeit("perf.%s(d)" % fn_name,
                      "import perf;"\
                      "d = perf.create_db(%s, %s, %s)" % (numvs, vsize, disable_compression),
                      number=number)
    print "%s values of size %s bytes" % (numvs, vsize)
    for disable_compression in (True, False):
        print "with%s compression" % ('out' if disable_compression else ' (possible)')
        print "\tlen\tsum\tsum / len"
        len_t = timer('len_all', disable_compression)
        sum_t = timer('sum_all', disable_compression)
        print "\t%.3f\t%.3f\t%.3f" % (len_t, sum_t, sum_t / len_t)

if __name__ == '__main__':
    time_db()
