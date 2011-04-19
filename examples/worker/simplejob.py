from disco.job import SimpleJob

class SimpleJob(SimpleJob):
    def map(self, worker, task, **jobargs):
        worker.output(task, partition=None).file.append('hello world!')

    def reduce(self, worker, task, **jobargs):
        worker.output(task, partition=None).file.append('goodbye world!')
