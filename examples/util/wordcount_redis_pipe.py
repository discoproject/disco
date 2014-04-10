"""
This example uses redis to store intermediate results.  The usage is quite
simple.  You have to create a redis_server function which returns
something like redis://server:port:dbid.

Then run the example:
    $ python wordcoun_redis_pipe.py ddfs_input_tag
If you use localhost like this example, each node is going to use its own
local redis server.

The outputs of the task can be stored in redis or read from redis
with using redis_inter_stream_[out|in] streams.  The critical decision is to
specify the correct redis_server for each task.  In the example, we are assuming
that:
    1. There is a redis server on each worker node.
    2. Each server has enough databases.  And by enough we mean at least as many as
    the number of tasks in that stage.  This can be achieved by starting redis
    servers with the 'databases N' option.

These redis key-value stores are usually accessed locally, but they still have
to support remote lookups.  For example, the outputs of a task might be consumed
in another node.  The consumer node will access the redis server of the producer
node.
The URI of these redis servers are communicated using Disco's basic input-output
mechanism.

Warning: All of the intermediate servers given to these tasks will be purged
before being used.
"""
from disco.core import Job, result_iterator
from disco.worker.pipeline.worker import Stage
from disco.worker.task_io import chain_reader, task_input_stream
from disco.schemes.scheme_redis import redis_inter_stream_out, redis_inter_stream_in
from functools import partial

def map(interface, state, label, inp):
    for line in inp:
        for word in line.split():
            interface.output(0).add(word, str(1))

def reduce(interface, state, label, inp):
    out = interface.output(0)
    currentWord = None
    for word, one in inp:
        if currentWord == None:
            currentWord = word
            count = 1
            continue
        if word == currentWord:
            count += 1
        else:
            out.add(currentWord, count)
            currentWord = word
            count = 1

    if currentWord != None:
        out.add(currentWord, count)

def redis_server(partition, url, params):
    dbid = url.split('/')[-2].split('-')[2]
    host = url.split('/')[-1].split('-')[-2]
    redis_server = "redis://" + host + ":6379:" + dbid
    return redis_server

class WordCount(Job):
    def __init__(self):
        from disco.worker.pipeline.worker import Worker
        super(WordCount, self).__init__(worker = Worker())
    pipeline = [("split", Stage("map", process=map,
                 input_chain = [task_input_stream, chain_reader],
                 output_chain = [partial(redis_inter_stream_out, redis_server=redis_server)],
                 )),
        ("group_label", Stage("reduce", process=reduce,
            input_chain = [task_input_stream, redis_inter_stream_in]))]

if __name__ == '__main__':
    from wordcount_redis_pipe import WordCount
    job = WordCount()
    import sys
    job.run(input=["tag://" + sys.argv[1]])
    for line, count in result_iterator(job.wait(show=False)):
        print(line, count)
