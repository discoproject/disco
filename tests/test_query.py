from disco.test import TestCase, TestPipe
from disco.compat import bytes_to_str, str_to_bytes
from disco.worker.pipeline.worker import Stage
from disco.worker.task_io import task_input_stream
import csv
from functools import partial
import hashlib

PREFIX='/tmp/'

def read(interface, state, label, inp):
    from disco import util
    for e in inp:
        scheme, netloc, _ = util.urlsplit(e)
        fileName, joinColumn = str(netloc).split('?')
        File = open(PREFIX + fileName, 'r')
        col = int(joinColumn)

        reader = csv.reader(File)
        firstRow = True
        for row in reader:
            if firstRow:
                tableName = row[0]
                firstRow = False
            else:
                fullName = tableName + '?' + str(col)
                Hash = int(hashlib.md5(str_to_bytes(row[col])).hexdigest(), 16) % 160
                interface.output(Hash).add(fullName, row)

def join_init(interface, params):
    return {}

def join(interface, state, label, inp):
    for k, v in inp:
        if k not in state:
            state[k] = [v]
        else:
            state[k].append(v)

def join_done(interface, state):
    if len(state) != 2:
        return

    name0 = list(state.keys())[0]
    name1 = list(state.keys())[1]

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
    #input contains the file name and the join column
    input = ['raw://cities.csv?0', 'raw://packages.csv?3']

    def SetUpFiles(self):
        F1 = open(PREFIX + 'cities.csv', 'w')
        F1.write("cities\nEdmonton,-45\nCalgary,-35\nMontreal,-25\nToronto,-15\n")
        F1.close()

        F2 = open(PREFIX + 'packages.csv', 'w')
        F2.write("packages\n0,2013-10-2,2013-11-3,Edmonton,Calgary\n" +
                 "1,2013-11-3,2013-12-3,Calgary,Toronto\n" +
                 "2,2013-10-4,2013-10-6,Edmonton,Montreal\n")
        F2.close()

    def serve(self, path):
        return path

    def test_per_node(self):
        self.SetUpFiles()
        self.job = PipeJob().run(input=self.test_server.urls(self.input))
        self.assertEqual(sorted(self.results(self.job)), [('Calgary', 1), ('Edmonton', 2)])
