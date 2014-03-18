from disco.core import Job, result_iterator
from disco.worker.task_io import task_output_stream, plain_output_stream

"""
In this example, we make the following assumptions:
    1. Input file is in INPUT_FILE.
    2. The user is USERR.
    3. We want the output file to be stored in OUTPUT_DIR.
    4. The user has read access to the input file and write access to the
        output directory.
    5. The Disco master and Hadoop NameNode are on the same machine.
"""
USER = "shayan"
OUTPUT_DIR = "/user/" + USER + "/"
INPUT_FILE = "/user/" + USER + "/chekhov"

def getHdfsMaster(discoMaster):
    from disco.util import schemesplit
    _, rest = schemesplit(discoMaster)
    return rest.split(':')[0] + ':50070'

class WordCount(Job):
    save = True
    reduce_output_stream = (task_output_stream, plain_output_stream)

    @staticmethod
    def map(line, params):
        for word in line.split():
            yield word, 1

    @staticmethod
    def reduce(iter, params):
        from disco.util import kvgroup
        for word, counts in kvgroup(sorted(iter)):
            yield word, sum(counts)

if __name__ == '__main__':
    from wordcount_hdfs import WordCount
    job = WordCount()
    job.save_info = "hdfs," + getHdfsMaster(job.disco.master) + "," + USER + "," + OUTPUT_DIR
    hdfsMaster = getHdfsMaster(job.disco.master)
    job = job.run(input=['hdfs://' + hdfsMaster + ':/' + INPUT_FILE])
    for word, count in result_iterator(job.wait(show=True)):
        print(word, count)
