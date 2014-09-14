# Copyright 2011 Jordan Andersen
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
This example has been ported from the mrjob project.

A brute force Map/Reduce solution to the Travelling Salesman Problem. The
purpose of this example is to demonstrate how to use Map/Reduce on
computationally intense problems that involve a relatively small input.

See the Wikipedia article for details of the problem:
http://en.wikipedia.org/wiki/Travelling_salesman_problem

The solution works by having each mapper find the shortest tour in a
chunk of the full range of the possible factorial(N-1) tours. (Where N is the
number of nodes in the graph). The reducers then pick the winners from each
mapper.
"""
from disco.core import Job, result_iterator
from disco.worker.classic.worker import Params
from math import factorial
import sys
import numpy
import json

def map_int_to_tour(num_nodes, i, start_node):
    """Gets a unique tour through a graph given an integer and starting node.

    Args:
    num_nodes -- the number of nodes in the graph being toured
    i -- the integer to be mapped to the set of tours for the graph
    start_node -- the node index to begin and end the tour on
    """
    nodes_remaining = range(0, start_node) + range(start_node + 1, num_nodes)
    tour = []

    while len(nodes_remaining) > 0:
        num_nodes = len(nodes_remaining)
        next_step = nodes_remaining[i % num_nodes]
        nodes_remaining.remove(next_step)
        tour.append(next_step)
        i = i / num_nodes

    tour = [start_node] + tour + [start_node]
    return tour

def cost_tour(graph, tour):
    """Calculates the travel cost of given tour through a given graph.

    Args:
    graph -- A square numpy.matrix representing the travel cost of each edge on
            the graph.
    tour -- A list of integers representing a tour through the graph where each
            entry is the index of a node on the graph.
    """
    steps = zip(tour[0:-1], tour[1:])
    cost = sum([graph[step_from, step_to] for step_from, step_to in steps])
    return cost

class TSPJob(Job):
    @staticmethod
    def map(e, params):
        """Mapper for step 2. Finds the shortest tour through a
        small range of all possible tours through the graph.

        At this step the key will contain a string describing the range of
        tours to cost. The sales_trip has the edge cost graph and the starting
        node in a dict.
        """

        shortest_length = sys.maxint
        shortest_path = []

        sales_trip = params.trip

        matrix = numpy.matrix(sales_trip['graph'])
        num_nodes = matrix.shape[0]
        ranges = e.split('-')
        range_low, range_high = int(ranges[0]), int(ranges[1])

        for i in range(range_low,range_high):

            tour = map_int_to_tour(num_nodes, i, sales_trip['start_node'])
            cost = cost_tour(matrix, tour)

            if cost < shortest_length:
                shortest_length = cost
                shortest_path = tour
        yield (None, (shortest_length, shortest_path))

    @staticmethod
    def reduce(iter, params):
        from disco.util import kvgroup
        for _, winners in kvgroup(sorted(iter)):
            yield min(winners)

if __name__ == '__main__':
    line = sys.stdin.readline()
    sales_trip = json.loads(line)
    m = numpy.matrix(sales_trip['graph'])
    num_nodes = m.shape[0]
    num_tours = factorial(num_nodes - 1)

    #Here we break down the full range of possible tours into smaller
    #pieces. Each piece is passed along as a key along with the trip
    #description.
    step_size = int(100 if num_tours < 100**2 else num_tours / 100)
    steps = range(0, num_tours, step_size) + [num_tours]
    ranges = zip(steps[0:-1], steps[1:])

    input = map(lambda x: 'raw://' + str(x[0]) + "-" + str(x[1]), ranges)

    from travelling_salesman import TSPJob
    job = TSPJob().run(input=input, params=Params(trip=sales_trip))
    for k, v in result_iterator(job.wait()):
        print k, v
