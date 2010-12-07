"""
:mod:`disco.error` -- Errors with special meaning in Disco
==========================================================


"""
import sys

class DiscoError(Exception):
    """The base class for all Disco errors"""
    pass

class JobError(DiscoError):
    """An error that occurs when a client submits or interacts with a Disco job.

    .. attribute:: error

        Error message.

    .. attribute:: name

        Name of the failed job.

    .. attribute:: master

        Address of the Disco master that produced the error.
    """
    def __init__(self, error, master=None, jobname=None):
        self.error   = error
        self.master  = master
        self.jobname = jobname

    def __str__(self):
        return "Job %s/%s failed: %s" % (self.master, self.jobname, self.error)

class DataError(DiscoError):
    """
    An error caused by an inability to access a data resource.

    These errors are treated specially by Disco master in that they are assumed to be recoverable.
    If Disco thinks an error is recoverable, it will retry the task on another node.
    """
    def __init__(self, msg, url, code=None):
        self.msg = msg
        self.url = url
        self.code = code

    def __str__(self):
        if self.code == None:
            return 'Unable to access resource (%s): %s' % (self.url, self.msg)
        else:
            return 'Unable to access resource (%s): %s (HTTP status %d)' %\
                (self.url, self.msg, self.code)

class CommError(DataError):
    """An error caused by the inability to access a resource over the network."""

class ModUtilImportError(DiscoError, ImportError):
    """An error raised by :mod:`disco.modutil` when it can't find a module."""
    def __init__(self, error, function):
        self.error    = error
        self.function = function

    def __str__(self):
        # XXX! Add module name below
        return ("%s: Could not find module defined in %s. Maybe it is a typo. "
                "See documentation of the required_modules parameter for details "
                "on how to include modules." % (self.error, self.function.func_name))
