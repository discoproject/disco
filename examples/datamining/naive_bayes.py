"""
Naive Bayes with MapReduce
===========================

This job is a simple implementation of the Naive Bayes algorithm with Disco.  It
uses a training dataset to create a likelihood dictionary and then based on this
dictionary classifies the second set of jobs.
See http://en.wikipedia.org/wiki/Naive_Bayes_classifier#Document_Classification
for more details about this usage of Naive Bayes.

The training data can be something like this:
$ cat train
    1 android cheap
    2 apple expensive
    3 android cheap
    4 android cheap

The first column is the id of the job.  The second column is the class this
entry belongs to and the third column is the feature of this entry.

The predicting dataset will be something like this:
$ cat predict
    1 cheap
    2 expensive

The first column shows the id of the entry and the second column shows its
feature.  Based on this feature, the predictor will output an indicator of the
likelihood of this entry belonging to each of the available classes.

For running this we can put the inputs in ddfs first:
    $ ddfs chunk train ./train
    $ ddfs chunk predict ./predict
    $ python naive_bayes.py 'tag://train' 'tag://predict' android apple

For me, it prints something like the following:
    ('1', 'android 2.4079456086518718')
    ('1', 'apple -2.4079456086518718')
    ('2', 'android 0.32850406697203571')
    ('2', 'apple -0.32850406697203571')
"""

from disco.worker.task_io import chain_reader
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from optparse import OptionParser

def estimate_map(e, params):
    L = e.split()
    z = dict([(elem, 1) for elem in L[1:]]).keys()
    x = [a for a in z if not a in params.ys]
    y = [a for a in z if a in params.ys]

    return [(b + params.splitter + a, 1) for a in x for b in y] + [(a, 1) for a in z] + [('', 1)]
#[(b+params.splitter,1) for b in y] + [(params.splitter+a,1) for a in x] + [(params.splitter,1)]

def estimate_combiner(k, v, counts, done, params):
    if done:
        return counts.items()
    if not counts.has_key(k):
        counts[k] = v
    else:
        counts[k] += v

def estimate_reduce(iter, out, params):
    counts = {}

    for k,v in iter:
        v = int(v)
        if not counts.has_key(k):
            counts[k] = v
        else:
            counts[k] += v

    for k,v in counts.iteritems():
        out.add(k, repr(v))

def predict_map(e, params):
    ll = dict([(k,params.loglikelihoods[k]) for k in params.ys.keys()])

    L = e.split()

    for elem in L[1:]:
        if params.ys.has_key(elem):
            continue

        for y in params.ys:
            ll[y] += params.loglikelihoods[y + params.splitter + elem]

    return [(L[0], k + ' ' + repr(ll[k])) for k in params.ys]

def estimate(input, ys, splitter = ' ', map_reader = chain_reader):
    ys = dict([(id, 1) for id in ys])

    job = Job(name = 'naive_bayes_estimate')

    job.run(input = input, map_reader = map_reader,
            map = estimate_map, combiner = estimate_combiner,
            reduce = estimate_reduce, params = Params(ys = ys, splitter = splitter),
            clean = False)
    results = job.wait()

    total = 0
    # will include the items for which we'll be classifying,
    # for example if the dataset includes males and females,
    # this dict will include the keys male and female and the
    # number of times these have been observed in the train set
    items = {}

    # the number of times the classes have been observed.  For
    # example,  if the feature is something like tall or short, then the dict
    # will contain the total number of times we have seen tall and short.
    classes = {}

    # the number of times we have seen a class with a feature.
    pairs = {}

    for key,value in result_iterator(results):
        l = key.split(splitter)
        value = int(value)
        if len(l) == 1:
            if l[0] == '':
                total = value
            elif ys.has_key(l[0]):
                classes[l[0]] = value
            else:
                items[l[0]] = value
        else:
            pairs[key] = value

#counts[key] = [[c,i], [not c, i], [c, not i], [not c, not i]]
    counts = {}
    for i in items:
        for y in ys:
            key = y + splitter + i
            counts[key] = [0, 0, 0, 0]
            if pairs.has_key(key):
                counts[key][0] = pairs[key]
            counts[key][1] = items[i] - counts[key][0]
            if not classes.has_key(y):
                counts[key][2] = 0
            else:
                counts[key][2] = classes[y] - counts[key][0]
            counts[key][3] = total - sum(counts[key][:3])

            # add pseudocounts
            counts[key] = map(lambda x: x + 1, counts[key])
    total += 4

    import math
    loglikelihoods = {}
    for key, value in counts.iteritems():
        l = key.split(splitter)
        if not loglikelihoods.has_key(l[0]):
            loglikelihoods[l[0]] = 0.0
        loglikelihoods[l[0]] += math.log(value[0] + value[2]) - math.log(value[1] + value[3])
        loglikelihoods[key] = math.log(value[0]) - math.log(value[1])

    return loglikelihoods

def predict(input, loglikelihoods, ys, splitter = ' ', map_reader = chain_reader):
    ys = dict([(id,1) for id in ys])
    job = Job(name = 'naive_bayes_predict')
    job.run(input = input, map_reader = map_reader,
            map = predict_map,
            params = Params(loglikelihoods = loglikelihoods, ys = ys, splitter = splitter),
            clean = False)
    return job.wait()

if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] inputs')
    (_, inputs) = parser.parse_args()
    ys = inputs[2:]
    loglikelihoods = estimate([inputs[0]], ys = ys)
    results = predict([inputs[1]], loglikelihoods, ys = ys)

    for e in result_iterator(results):
        print(e)
