from disco.test import TestCase, TestPipe
from disco.compat import bytes_to_str
from disco.worker.pipeline.worker import Stage
from disco.worker.task_io import task_input_stream
import sys
import csv
from functools import partial

def read(interface, state, label, inp):
    from disco import util
    for e in inp:
        scheme, netloc, _ = util.urlsplit(e)
        fileName, joinColumn = str(netloc).split('?')
        File = open('/tmp/' + fileName, 'r')
        col = int(joinColumn)

        reader = csv.reader(File)
        firstRow = True
        for row in reader:
            if firstRow:
                tableName = row[0]
                print "joinColumn is: ", str(col), " tableName is: ", tableName
                firstRow = False
            else:
                fullName = tableName + '?' + str(col)
                interface.output(hash(row[col]) % 160).add(fullName, row)

def join_init(interface, params):
    return {}

def join(interface, state, label, inp):
    for k, v in inp:
        if k not in state:
            state[k] = [v]
        else:
            state[k].append(v)

def join_done(interface, state):
    if len(state) > 2:
        sys.exit("Cannot join more than two state: %d given" % len(state))
    if len(state) < 2:
        print "Nothing to join here!"
        return

    name0 = state.keys()[0]
    name1 = state.keys()[1]
    _, strCol0 = name0.split('?')
    _, strCol1 = name1.split('?')
    col0 = int(strCol0)
    col1 = int(strCol1)

    for entry0 in state[name0]:
        for entry1 in state[name1]:
            if entry0[col0] == entry1[col1]:
                entry0_copy = entry0[:]
                entry1_copy = entry1[:]
                del entry0_copy[col0]
                del entry1_copy[col1]
                interface.output(0).add(entry0[col0], entry0_copy + entry1_copy)

def combine_init(interface, params, init):
    return init()

def combine(interface, state, label, inp, func):
    for k, v in inp:
        func(state, k, v)

def combine_done(interface, state):
    for k, v in state.items():
        interface.output(0).add(k, v)

def _getPipeline():
    select_stage = [("split", Stage('read', process=read))]
    join_stage = [("group_label", Stage('join', init=join_init, process=join, done=join_done))]

    def combine_row(state, k, v, func):
        if k not in state:
            state[k] = 0
        state[k] = state[k] + func(v)

    node_combine_stage = [("group_node_label",
            Stage('node_combine', init=partial(combine_init, init=lambda: {}),
                process=partial(combine, func=partial(combine_row, func=lambda v: 1)),
                done=combine_done))]

    combine_all_stage = [("group_label",
                Stage('combine_all', init=partial(combine_init, init=lambda: {}),
                    process=partial(combine, func=partial(combine_row, func=lambda v: v)),
                    done=combine_done))]

    return select_stage + join_stage + node_combine_stage + combine_all_stage

class PipeJob(TestPipe):
    pipeline = _getPipeline()

class JoinTestCase(TestCase):
    """
    The input files are cities.csv with the following content:
        cities
        Edmonton,-45
        Calgary,-35
        Montreal,-25
        Toronto,-15

    and packages.csv with the following content:
        packages
        0,2013-10-2,2013-11-3,Edmonton,Calgary
        1,2013-11-3,2013-12-3,Calgary,Toronto
        2,2013-10-4,2013-10-6,Edmonton,Montreal
    """
    #input contains the file name and the join column
    input = ['raw://cities.csv?0', 'raw://packages.csv?3']

    def serve(self, path):
        return path

    def test_per_node(self):
        self.job = PipeJob().run(input=self.test_server.urls(self.input))
        self.assertEqual(sorted(self.results(self.job)), [('Calgary', 1), ('Edmonton', 2)])
