
Release notes
=============

Disco 0.2.2 (July 26th 2009)
----------------------------

New features
''''''''''''

 - Experimental support for POSIX-compatible distributed filesystems, 
   in particular `GlusterFS <http://gluster.com>`_. Two modes are available: Disco
   can read input data from a distributed filesystem while preserving data locality
   (aka *inputfs*). Disco can also use a DFS for internal communication,
   replacing the need for node-specific web servers (aka *resultfs*). For more
   information, see :ref:`dfs`.


Bugfixes
''''''''

 - ``DISCO_PROXY`` handles now out-of-band results correctly (commit b1c0f9911)
 - `make-lighttpd-proxyconf.py` now ignores commented out lines in `/etc/hosts` (bug #14, commit a1a93045d) 
 - Fixed missing PID file in the `disco-master` script. The `/etc/init.d/disco-master` script in Debian packages now works correctly (commit 223c2eb01)
 - Fixed a regression in `Makefile`. Config files were not copied to `/etc/disco` (bug #13, commit c058e5d6)
 - Increased `server.max-write-idle` setting in Lighttpd config. This prevents the http connection from disconnecting with long running, cpu-intensive reduce tasks  (bug #12, commit 956617b0)


Disco 0.2.1 (May 26th 2009)
---------------------------

New features
''''''''''''

 - Support for redundant inputs: You can now specify many redundant addresses for an input file. Scheduler chooses the address which points at the node with the lowest load. If the address fails, other addresses are tried one by one until the task succeeds. See *inputs* in :meth:`disco.core.Disco.new_job` for more information.
 - Task profiling: See :ref:`profiling`
 - Implemented an efficient way to poll for results of many concurrent jobs. See :meth:`disco.core.Disco.results`.
 - Support for the `Curl <http://curl.haxx.se>`_ HTTP client library added. Curl is used by default if the ``pycurl`` module is available.
 - Improved storing of intermediate results: Results are now spread to a directory hierarchy based on the md5 checkum of the job name.

Bugfixes
''''''''

 - Check for ``ionice`` before using it. (commit dacbbbf785)
 - ``required_modules`` didn't handle submodules (PIL.Image etc.) correctly (commit a5b9fcd970)
 - Missing file balls.png added. (bug #7, commit d5617a788)
 - Missing and crashed nodes don't cause the job to fail (bug #2, commit 6a5e7f754b)
 - Default value for nr_reduces now never exceeds 100 (bug #9, commit 5b9e6924)
 - Fixed homedisco regression in 0.2. (bugs #5, #10, commit caf78f77356)

Disco 0.2 (April 7th 2009)
--------------------------

New features
''''''''''''

 - :ref:`oob`: A mechanism to produce auxiliary results in map/reduce tasks.
 - Map writers, reduce readers and writers (see :meth:`disco.core.Disco.new_job`): Support for custom result formats and internal protocols.
 - Support for arbitrary output types: :ref:`outputtypes`.
 - Custom task initialization functions: See *map_init* and *reduce_init* in :meth:`disco.core.Disco.new_job`.
 - Jobs without inputs i.e. generator maps: See the `raw://` protocol in :meth:`disco.core.Disco.new_job`.
 - Reduces without maps for efficient join and merge operations: See :ref:`reduceonly`.

Bugfixes
''''''''

(NB: bug IDs in 0.2 refer to the old bug tracking system)

 - ``chunked = false`` mode produced incorrect input files for the reduce phase (commit db718eb6)
 - Shell enabled for the disco master process (bug #7, commit 7944e4c8)
 - Added warning about unknown parameters in ``new_job()`` (bug #8, commit db707e7d)
 - Fix for sending invalid configuration data (bug #1, commit bea70dd4)
 - Fixed missing ``msg``, ``err`` and ``data_err`` functions (commit e99a406d)

