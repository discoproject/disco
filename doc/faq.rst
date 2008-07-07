
Disco FAQ
=========

.. contents::

Using Disco
-----------

How to maintain state across many map / reduce calls?
'''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the parameters object :class:`disco.Params` as the closure for your
functions. Here's an example::

        def fun_map(e, params):
                params.c += 1
                if not params.c % 10:
                        return [(e, "good")]
                else:
                        return [(e, "not good")]

        disco.job("disco://localhost:5000", 
                      ["disco://localhost/myjob/file1"],
                      fun_map,
                      params = disco.Params(c = 0))

In this case *params.c* is a counter variable that is incremented in
every call to the map function.

How to send log entries from my functions to the Web interface?
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Use the :func:`disco_worker.msg` function. Here's an example::

        def fun_map(e, params):
                params.c += 1
                if not c % 100000:
                        msg("Now processing %dth entry" % params.c)
                return [(e, 1)]

        disco.job("disco://localhost:5000", 
                  ["disco://localhost/myjob/file1"],
                  fun_map,
                  params = disco.Params(c = 0))

Note that you must not call :func:`disco_worker.msg` too often. If you send more
than 10 messages per second, Disco will kill your job.


Disco Internals
---------------
