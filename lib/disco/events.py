import os, re, sys
from datetime import datetime

from disco.json import dumps, loads

class Event(object):
    type             = 'EV'
    version          = '00'
    tag_re           = re.compile(r'^\w+$')
    timestamp_format = '%y/%m/%d %H:%M:%S'

    def __init__(self, payload='', tags=()):
        self.payload = payload
        self.tags    = tags
        self.time    = datetime.now()

    @property
    def timestamp(self):
        return self.time.strftime(self.timestamp_format)

    # TODO! Optimize read_head()!
    def read_head(self):
        head = ""
        while True:
            r = sys.stdin.read(1)
            if r == ' ':
                return head
            else:
                head += r

    def send(self):
        sys.stderr.write('%s' % self)
        status = self.read_head()
        bytes = self.read_head()
        body = loads(sys.stdin.read(int(bytes) + 1)[:-1])
        if status == 'ERROR':
            raise ValueError(body)
        return body

    def __str__(self):
        body = dumps(self.payload)
        return '%s %d %s\n' % (self.type, len(body), body)

class Status(Event):
    type = 'STA'

class Message(Event):
    type = 'MSG'

class AnnouncePID(Event):
    type = 'PID'

class DataUnavailable(Event):
    type = 'DAT'

class Input(Event):
    type = 'INP'

class JobFile(Event):
    type = 'JOB'

class Output(Event):
    type = 'OUT'

class TaskFailed(Event):
    type = 'ERR'

class TaskInfo(Event):
    type = 'TSK'

class WorkerDone(Event):
    type = 'END'

class EventRecord(object):
    type_raw      = r'\*\*<(?P<type>\w+)(?::(?P<version>.{2}))?>'
    timestamp_raw = r'(?P<timestamp>\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    tags_raw      = r'(?P<tags>[^\n]*)'
    payload_raw   = r'(?P<payload>.*)'
    event_re = re.compile(r'^%s %s %s\n%s\n<>\*\*\n$' % (type_raw, timestamp_raw, tags_raw, payload_raw),
                          re.MULTILINE | re.S)

    def __init__(self, string):
        match = self.event_re.match(string)
        if not match:
            raise TypeError("%s is not in Event format" % string)
        self.type    = match.group('type')
        self.time    = datetime.strptime(match.group('timestamp'), Event.timestamp_format)
        self.tags    = match.group('tags').split()
        self.payload = loads(match.group('payload'))
