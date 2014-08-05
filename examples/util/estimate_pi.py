# This program estimates the value of pi (3.14...)
# Usage:
# python estimate_pi.py

from disco.core import Job, result_iterator

def map(line, params):
    from random import random
    x, y = random(), random()
    yield 0, 1 if x*x + y*y < 1 else 0

if __name__ == '__main__':
    COUNT = 5000
    job = Job().run(input=["raw://0"] * COUNT , map=map)
    tot = 0
    for k, v in result_iterator(job.wait()):
        tot += v
    print (4.0 * tot) / COUNT
