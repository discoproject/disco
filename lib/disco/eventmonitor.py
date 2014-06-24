import sys, os, time, json, curses
from disco.error import DiscoError
from disco.compat import str_to_bytes, force_ascii

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
                curses.setupterm(fd=self.handle.fileno())
                return curses.tigetnum('colors') > 2
        except Exception as e:
            pass

    def write(self, *args, **kwargs):
        self.writer.write(*args, **kwargs)

    def cleanup(self):
        self.writer.cleanup()

class EventWriter(object):
    def __init__(self, handle):
        self.handle = handle

    def write(self, *args, **kwargs):
        pass

    def cleanup(self):
        pass

class TextEventWriter(EventWriter):
    def write(self, status=None, timestamp=None, host=None, message=None):
        if timestamp:
            self.handle.write('{0} {1} {2}\n'.format(timestamp, host, message))
        elif status:
            self.handle.write('{0}\n'.format(status))

class ANSIEventWriter(EventWriter):
    def __init__(self, handle):
        super(ANSIEventWriter, self).__init__(handle)
        #self.w = curses.initscr()

    def cleanup(self):
        # This effort at cleanup also clears the shown messages, which
        # does not preserve previous behavior.  For now, we disable
        # it, and live with the colored result output in Py3.

        #curses.echo()
        #curses.nocbreak()
        #curses.endwin()
        pass

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
        return self.background(bgcolor) + self.foreground(fgcolor) + str_to_bytes(text)

    def colorbar(self, length, color=WHITE):
        return self.ansi_text(' ' * length, bgcolor=color)

    def format(self, text):
        if text.startswith('ERROR'):
            return self.error(text)
        elif text.startswith('WARN'):
            return self.warning(text)
        elif text.startswith('READY'):
            return self.ready('{0} '.format(text))
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
         return self.ansi_text(' {0}'.format(ready), bgcolor=GREEN, fgcolor=WHITE) + self.end_line

    def status(self, status):
        return self.ansi_text(status, fgcolor=CYAN) + self.end_line

    def timestamp(self, timestamp):
        return self.ansi_text(timestamp, fgcolor=GREEN)

    def warning(self, warning):
        return self.ansi_text(warning, fgcolor=MAGENTA) + self.end_line

    def write(self, status=None, timestamp=None, host=None, message=None):
        message = force_ascii(message) if message else ''
        if status:
            return curses.putp(self.status(status + '\r\n') + self.end_line)

        if not timestamp:
            return curses.putp(self.heading(message + ":\r\n") + self.end_line)

        curses.putp(self.timestamp('{0} '.format(timestamp))
                    + self.host('{0:<10s} '.format(host))
                    + self.format(message + '\r\n')
                    + self.end_line)

class JSONEventWriter(EventWriter):
    def write(self, status=None, timestamp=None, host=None, message=None):
        if timestamp:
            print(json.dumps([timestamp, host, message]))


class EventMonitor(object):
    def __init__(self, job, format=None, poll_interval=2):
        self.job           = job
        self.offset        = 0
        self.poll_interval = poll_interval
        self.prev_status   = None
        self.output        = OutputStream(format)
        self.output.write(message=self.job.name)

    def cleanup(self):
        self.output.cleanup()

    @property
    def events(self):
        return self.job.events(self.offset)

    @property
    def isenabled(self):
        return self.output.isenabled

    @property
    def stats(self):
        pipeline = self.job.jobinfo()['pipeline']
        first = pipeline[0]
        pipeline.reverse()
        for stage in pipeline:
            if sum(stage[1:]): return stage
        return first

    @property
    def status(self):
        return ("Status: [{0[0]}] {0[1]} pending, {0[2]} waiting, {0[3]} running, {0[4]} done, {0[5]} failed"
                .format(tuple(self.stats)))

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
