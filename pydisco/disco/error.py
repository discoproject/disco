import sys

class DiscoError(Exception):
    pass

class JobError(DiscoError):
    """An error that occurs when a client submits or interacts with a disco job."""
    def __init__(self, error, master=None, jobname=None):
        self.error   = error
        self.master  = master
        self.jobname = jobname

    def __str__(self):
        return "Job %s/%s failed: %s" % (self.master, self.jobname, self.error)

class DataError(DiscoError):
    """An caused by an inability to access a data resource."""
    def __init__(self, msg, url):
        self.msg = msg
        self.url = url

    def __str__(self):
        return 'Unable to access resource (%s): %s' % (self.url, self.msg)

class CommError(DataError):
    """An error caused by the inability to access a resource over the network."""

class ModUtilImportError(DiscoError, ImportError):
    def __init__(self, error, function):
        self.error    = error
        self.function = function

    def __str__(self):
        return ("%s: Could not find module defined in %s. Maybe it is a typo. "
                "See documentation of the required_modules parameter for details "
                "on how to include modules." % (self.error, self.function.func_name))
