"""
K-clustering in MapReduce
=========================

"""

from disco.core import Disco, Params, result_iterator
from disco.func import chain_reader
from optparse import OptionParser
from sys import stderr
from os import getenv

def mean_point_create(x, w = 1.0):
    return { '_x':x, 'w': w }

def mean_point_update(p, q):
    p['_x'] = [ p['_x'][i]+q['_x'][i] for i in range(len(q['_x']))]
    p['w'] += q['w']

def mean_point_evaluate(p):
    p['x'] = [v/p['w'] for v in p['_x']]
    del p['_x']

def mean_point_dist(p, x):
    return sum((p['x'][i]-x[i])**2 for i in range(len(x)))

mean_point_center = {'create':mean_point_create, 'update':mean_point_update,
                     'evaluate':mean_point_evaluate, 'dist':mean_point_dist}

def map_init(iter, params):
    """Intialize random number generator with given seed `params.seed`."""
    random.seed(params.seed)

def random_init_map(e, params):
    """Assign datapoint `e` randomly to one of the `k` clusters."""
    yield (random.randint(0,params.k-1), e[1])

def estimate_map(e, params):
    """Find the cluster `i` that is closest to the datapoint `e`."""
    yield (min((params.dist(c, e[1]), i) for i,c in params.centers)[1], e[1])

def estimate_combiner(i, x, c, done, params):
    if done:
        return c.iteritems()

    if i not in c: c[i] = params.create(x)
    else: params.update(c[i], params.create(x))

def estimate_reduce(iter, out, params):
    centers = {}

    for i, c in iter:
        if i not in centers: centers[i] = c
        else: params.update(centers[i],c)

    for i, c in centers.iteritems():
        params.evaluate(c)
        out.add(i, c)

def predict_map(e, params):
    yield (e[0], min((params.dist(c,e[1]), i) for i,c in params.centers))

def estimate(master, input, k, iterations, center):
    """
    Optimize k-clustering for `iterations` iterations with cluster
    center definitions as given in `center`.
    """
    job = master.new_job(name = 'k-clustering_init',
                         input = input,
                         map_reader = chain_reader,
                         map_init = map_init,
                         map = random_init_map,
                         combiner = estimate_combiner,
                         reduce = estimate_reduce,
                         params = Params(k = k,
                                         create = center['create'],
                                         update = center['update'],
                                         evaluate = center['evaluate'],
                                         dist = center['dist'],
                                         seed = None),
                         nr_reduces = k)

    centers = [(i,c) for i,c in result_iterator(job.wait())]
    job.purge()

    for  j in range(iterations):
        job = master.new_job(name = 'k-clustering_iteration_%s' %(j,),
                             input = input,
                             map_reader = chain_reader,
                             map = estimate_map,
                             combiner = estimate_combiner,
                             reduce = estimate_reduce,
                             params = Params(centers = centers,
                                             create = center['create'],
                                             update = center['update'],
                                             evaluate = center['evaluate'],
                                             dist = center['dist']),
                             nr_reduces = k)

        centers = [(i,c) for i,c in result_iterator(job.wait())]
        job.purge()

    return centers


def predict(master, input, centers, center):
    job = master.new_job(name = 'kcluster_predict',
                         input = input,
                         map_reader = chain_reader,
                         map = predict_map,
                         params = Params(centers = centers,
                                         create = center['create'],
                                         update = center['update'],
                                         evaluate = center['evaluate'],
                                         dist = center['dist']),
                         nr_reduces = 0)

    return job.wait()


if __name__ == '__main__':
    parser = OptionParser(usage = '%prog [options] inputs')
    parser.add_option('--disco-master', default = getenv('DISCO_MASTER') or 'disco://disco2',
                      help = 'Disco master (default: disco://disco2)')
    parser.add_option('--iterations', default = 10,
                      help = 'Numbers of iteration (default: 10)')

    parser.add_option('--clusters', default = 10,
                      help = 'Numbers of clusters (default: 10)')

    (options, input) = parser.parse_args()

    master = Disco(options.disco_master)

    centers = estimate(master, input, int(options.clusters), int(options.iterations), mean_point_center)

    res = predict(master, input, centers, mean_point_center)

    print '\n'.join(res)
