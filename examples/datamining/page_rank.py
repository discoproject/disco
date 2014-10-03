# Copyright 2009-2010 Yelp
# Copyright 2013 David Marin
# Copyright 2014 Disco Project
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Iterative implementation of the PageRank algorithm:

This example has been ported from the mrjob project.
http://en.wikipedia.org/wiki/PageRank

The format of the input should be of the form:
node_id initial_score neighbor_1 weight_1 neighbor_2 weight_2 ...

For example, the following input is derieved from wikipedia:

$ cat input

0 1
1 1 2 1
2 1 1 1
3 1 0 0.5 1 0.5
4 1 1 0.33 3 0.33 5 0.33
5 1 1 0.5 4 0.5
6 1 1 0.5 4 0.5
7 1 1 0.5 4 0.5
8 1 1 0.5 4 0.5
9 1 4 1
10 1 4 1

$ cat input | ddfs chunk pages -
$ python page_rank.py --iterations 10 pages


The results are:

0 : 0.303085470793
1 : 3.32372143585
2 : 3.39335760361
3 : 0.360345571947
4 : 0.749335470793
5 : 0.360345571947
6 : 0.15
7 : 0.15
8 : 0.15
9 : 0.15
10 : 0.15

"""
from optparse import OptionParser
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from disco.worker.task_io import chain_reader

def send_score(line, params):
    """Mapper: send score from a single node to other nodes.

    Input: ``node_id, node``

    Output:
    ``node_id, ('n', node)`` OR
    ``node_id, ('s', score)``
    """

    if not isinstance(line, str):
        line = line[1]

    fields = line.split()
    node_id = int(fields[0])
    score = float(fields[1])


    yield node_id, ("n", " ".join(fields[2:]))

    for i in range(2, len(fields), 2):
        dest_id = int(fields[i])
        weight = float(fields[i+1])
        yield dest_id, ("s", score * weight)

def receive_score(iter, params):
    from disco.util import kvgroup
    d = params.damping_factor
    for node_id, vals in kvgroup(sorted(iter)):
        sum_v = 0
        neighbors = None
        for t, v in vals:
            if t == "s":
                sum_v += v
            else:
                neighbors = v
        score = 1 - d + d * sum_v
        yield node_id, str(node_id) + " " + str(score) + " " + neighbors

if __name__ == '__main__':
    parser = OptionParser(usage='%prog [options] inputs')
    parser.add_option('--iterations',
                      default=10,
                      help='Numbers of iteration')
    parser.add_option('--damping-factor',
                      default=0.85,
                      help='probability a web surfer will continue clicking on links')

    (options, input) = parser.parse_args()

    results = input

    params = Params(damping_factor=float(options.damping_factor))

    for j in range(int(options.iterations)):
        job = Job().run(input=results, map=send_score, map_reader = chain_reader, reduce=receive_score, params = params)
        results = job.wait()

    for _, node in result_iterator(results):
        fields = node.split()
        print fields[0], ":", fields[1]
