from disco import schemes
from disco.util import inputlist, shuffled
from disco.worker import SerialInput

def open(url, task=None):
    partition = str(task.taskid) if task else None
    return SerialInput(shuffled(inputlist([url], partition=partition)),
                       open=lambda url: schemes.open_chain(url, task=task))
