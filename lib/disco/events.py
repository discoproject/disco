import os, re, sys
from datetime import datetime
from binascii import hexlify

class Event(object):
    type             = 'EV'
    tag_re           = re.compile(r'^\w+$')
    timestamp_format = '%y/%m/%d %H:%M:%S'

    def __init__(self, message='', tags=()):
        self.message = message
        self.tags    = tags
        self.time    = datetime.now()
        self.log()

    @property
    def timestamp(self):
        return self.time.strftime(self.timestamp_format)

    def log(self):
        sys.stderr.write('%s' % self)

    def __str__(self):
        tags = ' '.join(tag for tag in self.tags if self.tag_re.match(tag))
        msg = '**<%s> %s %s\n%s\n<>**\n' % (self.type, self.timestamp, tags, self.message)
        try:
            return msg.encode('utf-8')
        except UnicodeError:
            hexmsg = hexlify(self.message)
            msg = ('**<%s> %s %s\nWarning: non-UTF8 output from worker: %s\n<>**\n'
                   % (self.type, self.timestamp, tags, hexmsg))
            return msg

class Status(Event):
    type = 'STA'

class Message(Event):
    type = 'MSG'

class Signal(Event):
    pass

class AnnouncePID(Signal):
    type = 'PID'

class DataUnavailable(Signal):
    type = 'DAT'

class WorkerDone(Signal):
    type = 'END'

class TaskFailed(Signal):
    type = 'ERR'

class OutputURL(Signal):
    type = 'OUT'

class EventRecord(object):
    type_raw      = r'\*\*<(?P<type>\w+)>'
    timestamp_raw = r'(?P<timestamp>\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})'
    tags_raw      = r'(?P<tags>[^\n]*)'
    message_raw   = r'(?P<message>.*)'
    event_re = re.compile(r'^%s %s %s\n%s\n<>\*\*\n$' % (type_raw, timestamp_raw, tags_raw, message_raw),
                          re.MULTILINE | re.S)

    def __init__(self, string):
        match = self.event_re.match(string)
        if not match:
            raise TypeError("%s is not in Event format" % string)
        self.type    = match.group('type')
        self.time    = datetime.strptime(match.group('timestamp'), Event.timestamp_format)
        self.tags    = match.group('tags').split()
        self.message = match.group('message')
