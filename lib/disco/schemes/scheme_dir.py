from disco import schemes
from disco.util import inputlist, shuffled
from disco.worker import SerialInput

def open(url, task=None):
    label = task.group_label if task else None
    return SerialInput(shuffled(inputlist([url], label=label)),
                       open=lambda url: schemes.open_chain(url, task=task))
