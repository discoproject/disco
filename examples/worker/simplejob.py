from disco.job import SimpleJob

class SimpleJob(SimpleJob):
    def map(self, worker, task, **jobargs):
        task.output(None).file.append('hello world!')

    def reduce(self, worker, task, **jobargs):
        task.output(None).file.append('goodbye world!')
