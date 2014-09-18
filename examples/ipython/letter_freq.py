def map(line, params):
    for char in line.lower():
        if char >= 'a' and char <= 'z':
            yield char, 1

def reduce(iter, params):
    from disco.util import kvgroup
    for char, counts in kvgroup(sorted(iter)):
        yield char, sum(counts)

# run the disco job
from disco.core import Job, result_iterator
job = Job().run(input=["http://en.wikipedia.org/wiki/MapReduce"], map=map, reduce=reduce)

# plot the results with matplotlib
#%matplotlib inline
xs, ys = zip(*result_iterator(job.wait()))
import scipy
from matplotlib import pylab
x = scipy.arange(len(xs))
y = scipy.array(ys)
f = pylab.figure()
ax = f.add_axes([0, 0, 3, 1])
ax.bar(x, y, align='center')
ax.set_xticks(x)
ax.set_xticklabels(xs)
f.show()
