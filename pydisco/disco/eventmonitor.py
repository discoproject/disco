import sys, cjson
from os import environ

# Following from Python cookbook, #475186
def has_colors(stream):
        global curses
        if not hasattr(stream, "isatty"):
                return False
        if not stream.isatty():
                return False # auto color only on TTYs
        try:
                import curses
                curses.setupterm()
                return curses.tigetnum("colors") > 2
        except:
                # guess false in case of error
                return False


class EventMonitor(object):
        def __init__(self, show, disco = None, name = None, job = None):
                if job:
                        self.disco = job.master
                        self.name = job.name
                else:
                        self.disco = disco
                        self.name = name

                if not disco:
                        raise Exception("Specify either job or disco and name")

                self.offset = 0
                self.output = None
                self.prev_status = None

                if show == None:
                        show = environ.get("DISCO_EVENTS", None)
                if show:
                        if type(show) == str:
                                show = show.lower()
                        if show == "json":
                                self.output = self.json_output
                        elif not has_colors(sys.stdout) or show == "nocolor":
                                self.output = self.plain_output
                        else:
                                self.init_ansi()
                                self.output = self.ansi_output
                if self.output:
                        self.output(None, None, None, self.name)

        def init_ansi(self):
                fg = curses.tigetstr("setaf")
                bg = curses.tigetstr("setab")
                self.reset = curses.tigetstr("sgr0")
                el = curses.tigetstr("el")
                new_bg = curses.tparm(bg, 4)
                msg_bg = curses.tparm(bg, 7)
                done_bg = curses.tparm(bg, 2)
                self.new = lambda x:\
                        new_bg + curses.tparm(fg, 7) + x + msg_bg + el 
                self.status = lambda x: msg_bg + curses.tparm(fg, 6) + x + el
                self.tstamp = lambda x: msg_bg + curses.tparm(fg, 2) + x
                self.host = lambda x: msg_bg + curses.tparm(fg, 4) + x 
                self.msg = lambda x: msg_bg + curses.tparm(fg, 0) + x + el
                self.error = lambda x: msg_bg + curses.tparm(fg, 1) + x + el
                self.warn = lambda x: msg_bg + curses.tparm(fg, 5) + x + el
                self.done = lambda x: "%s %s%s%s%s%s" %\
                        (msg_bg, done_bg, curses.tparm(fg, 7), x, msg_bg, el)

        def isenabled(self):
                return self.output != None

        def refresh(self):
                if not self.output:
                        return
                info = self.disco.jobinfo(self.name)
                if sum(info['redi'][1:]):
                        n = tuple(["reduce"] + info['redi'])
                else:
                        n = tuple(["map"] + info['mapi'])
                
                status = "Status: [%s] %d waiting, %d running, "\
                         "%d done, %d failed" % n

                i = 0
                for offs, event in self.disco.events(self.name, self.offset):
                        self.offset = offs
                        tstamp, host, msg = event
                        self.output(None, tstamp, host, msg)
                        i += 1
                if i and self.prev_status != status:
                        self.output(status, None, None, None)
                        self.prev_status = status


        def plain_output(self, status, tstamp, host, msg):
                if tstamp:
                        print tstamp, host, msg
                elif status:
                        print status

        def json_output(self, status, tstamp, host, msg):
                if tstamp:
                        print cjson.encode([tstamp, host, msg])
        
        def ansi_output(self, status, tstamp, host, msg):
                if status:
                        print self.status(status) + self.reset
                        return
                elif not tstamp:
                        print self.new(msg + ":") + self.reset
                        return

                msg = " " + msg
                if msg.startswith(" ERROR"):
                        m = self.error
                elif msg.startswith(" WARN"):
                        m = self.warn
                elif msg.startswith(" READY"):
                        m = self.done
                        msg = msg + " "
                else:
                        m = self.msg
                txt = "\n".join(m(x) for x in (msg).splitlines())
                print "%s%s%s%s" % (self.tstamp(tstamp + " "),\
                        self.host("%-10s" % host), txt, self.reset)






