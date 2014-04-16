"""
K-clustering with Pipelines
===========================
See the comments on top of kclustering.py for more info.

In this module, the same algorithm as kclustering is implemented with the
pipelines.
"""

from disco.core import Disco, result_iterator
from disco.worker.task_io import chain_reader, task_input_stream
from optparse import OptionParser
from os import getenv
from disco.job import Job
from disco.worker.pipeline.worker import Stage

def reader(fd, size, url):
    from disco.worker.task_io import chain_reader
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

def simple_init(interface, params):
    return params

def map_init(interface, params):
    """Intialize random number generator with given seed `params.seed`."""
    import random
    random.seed(params['seed'])
    return params

def random_init_map(interface, state, label, inp):
    """Assign datapoint `e` randomly to one of the `k` clusters."""
    import random
    out = interface.output(0)
    for e in inp:
        out.add(random.randint(0, state['k']-1), state['create'](e[1],1.0))

def estimate_map(interface, state, label, inp):
    """Find the cluster `i` that is closest to the datapoint `e`."""
    out = interface.output(0)
    for e in inp:
        Min = min((state['dist'](c, e[1]), i) for i,c in state['centers'])
        out.add(Min[1], state['create'](e[1],1.0))

def estimate_reduce(interface, state, label, inp):
    """Estimate the cluster centers for each cluster."""
    centers = {}
    for i, c in inp:
        centers[i] = c if i not in centers else state['update'](centers[i], c)

    out = interface.output(0)
    for i, c in centers.items():
        out.add(i, state['finalize'](c))

class Estimate(Job):
    def __init__(self):
        from disco.worker.pipeline.worker import Worker
        super(Estimate, self).__init__(worker = Worker())

def estimate(master, input, center, k, iterations):
    """
    Optimize k-clustering for `iterations` iterations with cluster
    center definitions as given in `center`.
    """
    from kclustering_pipeline import Estimate
    job = Estimate()
    job.pipeline = [("split",
                 Stage("k_cluster_init_map", input_chain =
                     [task_input_stream, reader], init = map_init,
                       process = random_init_map)),
                ('group_label',
                 Stage("k_cluster_init_reduce", process = estimate_reduce, init = simple_init))]
    job.params = center
    job.params['seed'] = 0
    job.params['k'] = k


    job.run(input = input)
    centers = [(i,c) for i,c in result_iterator(job.wait())]
    job.purge()

    for j in range(iterations):
        job = Estimate()
        job.params = center
        job.params['k'] = k
        job.params['centers'] = centers

        job.pipeline = [('split', Stage("kcluster_map_iter_%s" %(j,),
                input_chain = [task_input_stream, reader],
                process=estimate_map, init = simple_init)),
            ('group_label', Stage("kcluster_reduce_iter_%s" %(j,),
                process=estimate_reduce, init = simple_init))]
        job.run(input = input)
        centers = [(i,c) for i,c in result_iterator(job.wait())]
        job.purge()

    return centers

def predict_map(interface, state, label, inp):
    """Determine the closest cluster for the datapoint `e`."""
    out = interface.output(0)
    for e in inp:
        out.add(e[0], min((state['dist'](c,e[1]), i) for i,c in state['centers']))

def predict(master, input, center, centers):
    """
    Predict the closest clusters for the datapoints in input.
    """
    from kclustering_pipeline import Estimate
    job = Estimate()
    job.pipeline = [("split",
                 Stage("k_cluster_predict", input_chain =
                     [task_input_stream, reader], init = simple_init,
                       process = predict_map))]
    job.params = center
    job.params['centers'] = centers
    job.run(input = input)

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
