import sys, os, time
from disco import json
from disco.error import DiscoError

try:
    import curses
except ImportError:
    curses = None

BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)

class OutputStream(object):
    def __init__(self, format, handle=sys.stderr):
        self.handle    = handle
        self.isenabled = True

        if not format:
            self.isenabled = False
            self.writer    = EventWriter(handle)
        elif format == 'json':
            self.writer    = JSONEventWriter(handle)
        elif format == 'nocolor' or not self.hascolor:
            self.writer    = TextEventWriter(handle)
        else:
            self.writer    = ANSIEventWriter(handle)

    @property
    def hascolor(self): # Based on Python cookbook, #475186
        try:
            if self.handle.isatty():
                curses.setupterm()
                return curses.tigetnum('colors') > 2
        except Exception, e:
            pass

    def write(self, *args, **kwargs):
        self.writer.write(*args, **kwargs)

class EventWriter(object):
    def __init__(self, handle):
        self.handle = handle

    def write(self, *args, **kwargs):
        pass

class TextEventWriter(EventWriter):
    def write(self, status=None, timestamp=None, host=None, message=None):
        if timestamp:
            self.handle.write('%s %s %s\n' % (timestamp, host, message))
        elif status:
            self.handle.write('%s\n' % status)

class ANSIEventWriter(EventWriter):
    def __init__(self, handle):
        super(ANSIEventWriter, self).__init__(handle)

    @staticmethod
    def background(color):
        return curses.tparm(curses.tigetstr('setab'), color)

    @staticmethod
    def foreground(color):
        return curses.tparm(curses.tigetstr('setaf'), color)

    @property
    def reset(self):
        return curses.tigetstr('sgr0')

    @property
    def end_line(self):
        return self.reset + curses.tigetstr("el")

    def ansi_text(self, text, bgcolor=WHITE, fgcolor=BLACK):
        return self.background(bgcolor) + self.foreground(fgcolor) + text

    def colorbar(self, length, color=WHITE):
        return self.ansi_text(' ' * length, bgcolor=color)

    def format(self, text):
        if text.startswith('ERROR'):
            return self.error(text)
        elif text.startswith('WARN'):
            return self.warning(text)
        elif text.startswith('READY'):
            return self.ready('%s ' % text)
        return self.message(text)

    def error(self, error):
        return self.ansi_text(error, fgcolor=RED) + self.end_line

    def host(self, host):
        return self.ansi_text(host, fgcolor=BLUE)

    def message(self, message):
        return self.ansi_text(message) + self.end_line

    def heading(self, heading):
        return self.ansi_text(heading, bgcolor=BLUE, fgcolor=WHITE) + self.end_line

    def ready(self, ready):
         return self.ansi_text(' %s' % ready, bgcolor=GREEN, fgcolor=WHITE) + self.end_line

    def status(self, status):
        return self.ansi_text(status, fgcolor=CYAN) + self.end_line

    def timestamp(self, timestamp):
        return self.ansi_text(timestamp, fgcolor=GREEN)

    def warning(self, warning):
        return self.ansi_text(warning, fgcolor=MAGENTA) + self.end_line

    def write(self, status=None, timestamp=None, host=None, message=None):
        if message:
            message = message.encode('ascii', 'replace')
        if status:
            return self.handle.write('%s\n' % self.status(status))

        if not timestamp:
            return self.handle.write('%s\n' % self.heading(message + ":"))

        self.handle.write('%s %s %s\n' % (self.timestamp('%s ' % timestamp),
                          self.host('%-10s' % host),
                          self.format(message)))

class JSONEventWriter(EventWriter):
    def write(self, status=None, timestamp=None, host=None, message=None):
        if timestamp:
            print json.dumps([timestamp, host, message])


class EventMonitor(object):
    def __init__(self, job, format=None, poll_interval=2):
        self.job           = job
        self.offset        = 0
        self.poll_interval = poll_interval
        self.prev_status   = None
        self.output        = OutputStream(format)
        self.output.write(message=self.job.name)

    @property
    def events(self):
        return self.job.events(self.offset)

    @property
    def isenabled(self):
        return self.output.isenabled

    @property
    def stats(self):
        jobinfo = self.job.jobinfo()
        if sum(jobinfo['redi'][1:]):
            return ['reduce'] + jobinfo['redi']
        return ['map'] + jobinfo['mapi']

    @property
    def status(self):
        return "Status: [%s] %d waiting, %d running, %d done, %d failed" % tuple(self.stats)

    def log_events(self):
        for offset, (timestamp, host, message) in self.events:
            self.offset = offset
            self.output.write(timestamp=timestamp, host=host, message=message)

    def refresh(self):
        if self.isenabled:
            status = self.status
            self.log_events()
            if self.prev_status != status:
                self.output.write(status=status)
                self.prev_status = status
            time.sleep(self.poll_interval)
