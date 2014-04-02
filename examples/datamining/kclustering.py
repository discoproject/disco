"""
K-clustering with MapReduce
===========================
The input of this job should be something like this:

$ cat input
1 1 4
2 1 3
3 2 3
4 6 1
5 7 1
6 7 2

Where the first column is the id of the point and the remaining columns are
the values of the point in d dimensions (So we have d+1 columns for a
d-dimensional space). These dimensions should be separated with only
whitespaces, if the input is in another format, you have to change the reader
function to parse the input lines and yield a key-value pair containing the
point-id as the key and the rest as the value, like:
    (id, [x, y, z, ...])

If we want to use ddfs for the input dataset, we should first chunk the data
into ddfs under a tag name:

$ ddfs chunk cluster ./input

and then we can use the tag as the input to the job:

$ python kclustering.py --clusters 2 --iterations 3  'tag://cluster'

And it finds the two clusters for these points:
    ('1', (0.55555555555555536, 0))
    ('2', (0.22222222222222227, 0))
    ('3', (0.5555555555555558, 0))
    ('4', (0.55555555555555591, 1))
    ('5', (0.22222222222222199, 1))
    ('6', (0.55555555555555547, 1))

The first item in the value is the euclidean distance between the node and the
average and the second item is the cluster id.
"""

from disco.core import Disco, Params, result_iterator
from disco.worker.task_io import chain_reader
from optparse import OptionParser
from os import getenv

def reader(fd, size, url, params):
    for e in chain_reader(fd, size, url):
        L = e.split()
        yield L[0], [float(s) for s in L[1:]]

# HACK: The following dictionary will be transformed into a class once
# class support in Params has been added to Disco.
mean_point_center = {
    'create':(lambda x,w: { '_x':x, 'w':w }),
    'update':(lambda p,q: { '_x':[ pxi+qxi for pxi,qxi in zip(p['_x'],q['_x']) ], 'w':p['w']+q['w'] }),
    'finalize':(lambda p: { 'x':[v/p['w'] for v in p['_x']],'_x':p['_x'], 'w':p['w'] }),
    'dist':(lambda p,x: sum((pxi-xi)**2 for pxi,xi in zip(p['x'],x)) )
    }


def map_init(iter, params):
    """Intialize random number generator with given seed `params.seed`."""
    random.seed(params.seed)

def random_init_map(e, params):
    """Assign datapoint `e` randomly to one of the `k` clusters."""
    yield (random.randint(0,params.k-1), params.create(e[1],1.0))

def estimate_map(e, params):
    """Find the cluster `i` that is closest to the datapoint `e`."""
    yield (min((params.dist(c, e[1]), i) for i,c in params.centers)[1], params.create(e[1],1.0))

def estimate_combiner(i, c, centers, done, params):
    """Aggregate the datapoints in each cluster."""
    if done:
        return centers.iteritems()

    centers[i] = c if i not in centers else params.update(centers[i], c)

def estimate_reduce(iter, out, params):
    """Estimate the cluster centers for each cluster."""
    centers = {}
    for i, c in iter:
        centers[i] = c if i not in centers else params.update(centers[i], c)

    for i, c in centers.iteritems():
        out.add(i, params.finalize(c))

def predict_map(e, params):
    """Determine the closest cluster for the datapoint `e`."""
    yield (e[0], min((params.dist(c,e[1]), i) for i,c in params.centers))

def estimate(master, input, center, k, iterations, map_reader = reader):
    """
    Optimize k-clustering for `iterations` iterations with cluster
    center definitions as given in `center`.
    """
    job = master.new_job(name = 'k-clustering_init',
                         input = input,
                         map_reader = map_reader,
                         map_init = map_init,
                         map = random_init_map,
                         combiner = estimate_combiner,
                         reduce = estimate_reduce,
                         params = Params(k = k, seed = None,
                                         **center),
                         nr_reduces = k)

    centers = [(i,c) for i,c in result_iterator(job.wait())]
    job.purge()

    for  j in range(iterations):
        job = master.new_job(name = 'k-clustering_iteration_%s' %(j,),
                             input = input,
                             map_reader = map_reader,
                             map = estimate_map,
                             combiner = estimate_combiner,
                             reduce = estimate_reduce,
                             params = Params(centers = centers,
                                             **center),
                             nr_reduces = k)

        centers = [(i,c) for i,c in result_iterator(job.wait())]
        job.purge()

    return centers


def predict(master, input, center, centers, map_reader = reader):
    """
    Predict the closest clusters for the datapoints in input.
    """
    job = master.new_job(name = 'kcluster_predict',
                         input = input,
                         map_reader = map_reader,
                         map = predict_map,
                         params = Params(centers = centers,
                                         **center),
                         nr_reduces = 0)

    return job.wait()


if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] inputs')
    parser.add_option('--disco-master',
                      default=getenv('DISCO_MASTER'),
                      help='Disco master')
    parser.add_option('--iterations',
                      default=10,
                      help='Numbers of iteration')
    parser.add_option('--clusters',
                      default=10,
                      help='Numbers of clusters')

    (options, input) = parser.parse_args()
    master = Disco(options.disco_master)

    centers = estimate(master, input, mean_point_center,
                       int(options.clusters), int(options.iterations))

    res = predict(master, input, mean_point_center, centers)

    for e in result_iterator(res):
        print(e)
