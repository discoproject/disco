import sys

from disco.events import Event, Message, DataUnavailable

class DiscoError(Exception):
    pass

class JobException(DiscoError):
    def __init__(self, error, master=None, jobname=None):
        self.error   = error
        self.master  = master
        self.jobname = jobname

    def __str__(self):
        return "Job %s/%s failed: %s" % (self.master, self.jobname, self.error)

class CommException(DiscoError):
    def __init__(self, msg, url = ""):
        self.msg = msg
        self.url = url

    def __str__(self):
        return "HTTP exception (%s): %s" % (self.url, self.msg)

class ModUtilImportError(DiscoError, ImportError):
    def __init__(self, error, function):
        self.error    = error
        self.function = function

    def __str__(self):
        return ("%s: Could not find module defined in %s. Maybe it is a typo. "
                "See documetation of the required_modules parameter for details "
                "on how to include modules." % (self.error, self.function.func_name))

class JobError(DiscoError):
    def __init__(self, msg, cause=None):
        self.msg   = msg
        self.cause = cause
        self.log_event()

    def log_event(self):
        return Message(str(self))

    def __str__(self):
        if not self.cause:
            return self.msg
        return '%s: %s' % (self.msg, self.cause)

class DataError(JobError):
    def __init__(self, msg, data_url, cause=None):
        self.data_url = data_url
        super(DataError, self).__init__('%s (%s)' % (msg, data_url), cause=cause)

    def log_event(self):
        return DataUnavailable(self.data_url)
