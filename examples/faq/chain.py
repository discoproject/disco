from disco.job import Job
from disco.worker.task_io import chain_reader

class FirstJob(Job):
    input = ['raw://0', 'raw://0']

    @staticmethod
    def map(line, params):
        yield int(line) + 1, ""

class ChainJob(Job):
    map_reader = staticmethod(chain_reader)

    @staticmethod
    def map(key_value, params):
        yield int(key_value[0]) + 1, key_value[1]

if __name__ == "__main__":
    # Jobs cannot belong to __main__ modules.  So, import this very
    # file to access the above classes.
    import chain
    last = chain.FirstJob().run()
    for i in range(9):
        last = chain.ChainJob().run(input=last.wait())
    print(last.name)
