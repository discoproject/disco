.. _worker_protocol:

The Worker Message Protocol
===========================

.. hint:: Workers should not write anything to stderr.
          The worker uses stderr to communicate with the master.
          You can raise a :class:`disco.error.DataError`,
          to abort the worker and try again on another host.
          It is usually best to let the worker fail if any exceptions occur:
          do not catch any exceptions from which you can't recover.
          When exceptions occur, the disco worker will catch them and
          signal an appropriate event to the master.
