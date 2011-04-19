"""
:mod:`disco.error` -- Errors with special meaning in Disco
==========================================================


"""
import sys

class DiscoError(Exception):
    """The base class for all Disco errors"""
    pass

class JobError(DiscoError):
    """
    An error that occurs when an invalid job request is made.
    Instances of this error have the following attributes:

    .. attribute:: job

        The :class:`disco.job.Job` for which the request failed.

    .. attribute:: message

        The error message.
    """
    def __init__(self, job, message):
        self.job, self.message = job, message

    def __str__(self):
        return "Job %s failed: %s" % (self.job.name, self.message)

class DataError(DiscoError):
    """
    An error caused by an inability to access a data resource.

    These errors are treated specially by Disco master in that they are assumed to be recoverable.
    If Disco thinks an error is recoverable, it will retry the task on another node.
    If the same task fails on several different nodes, the master may terminate the job.
    """
    def __init__(self, msg, url, code=None):
        self.msg = msg
        self.url = url
        self.code = code

    def __str__(self):
        def msg(msg):
            return msg if self.code is None else '%s (%s)' % (msg, self.code)
        return 'Unable to access resource (%s): %s' % (self.url, msg(self.msg))

class CommError(DataError):
    """An error caused by the inability to access a resource over the network."""
