
Release notes
=============

Disco 0.2.4 (February 8th 2010)
-------------------------------

New features
''''''''''''

 - New fair job scheduler which replaces the old FIFO queue. The scheduler is
   inspired by `Hadoop's Fair Scheduler <http://hadoop.apache.org/common/docs/r0.20.1/fair_scheduler.html>`_.
   Running multiple jobs in parallel is now supported properly.
 - *Scheduler* option to control data locality and resource usage. See :meth:`disco.core.Disco.new_job`.
 - Support for custom input and output streams in tasks: See *map_input_stream*, *map_output_stream*,
   *reduce_input_stream* and *reduce_output_stream* in :meth:`disco.core.Disco.new_job`.
 - :meth:`disco.core.Disco.blacklist` and :meth:`disco.core.Disco.whitelist`.
 - New test framework based on Python's unittest module.
 - Improved exception handling.
 - Improved IO performance thanks to larger IO buffers.
 - Lots of internal changes.

Bugfixes
''''''''

 - Set ``LC_ALL=C`` for disco worker to ensure that external sort produces
   consistent results (bug #36, 7635c9a)
 - Apply rate limit to all messages on stdout / stderr. (bug #21, db76c80)
 - Fixed *flock* error handing for OS X (b06757e4)
 - Documentation fixes (bug #34, #42 9cd9b6f1)
   

Disco 0.2.3 (September 9th 2009)
--------------------------------

New features
''''''''''''

 - The :mod:`disco.settings` control script makes setting up and running Disco much easier than
   before.
 - Console output of job events (`screenshot
   <http://discoproject.org/img/disco-events.png>`_). You can now follow progress of a job
   on the console instead of the web UI by setting ``DISCO_EVENTS=1``. 
   See :meth:`disco.core.Disco.events` and :meth:`disco.core.Disco.wait`.
 - Automatic inference and distribution of dependent modules. See :mod:`disco.modutil`.
 - *required_files* parameter added to :meth:`disco.core.Disco.new_job`.
 - Combining the previous two features, a new easier way to use external C
   libraries is provided, see :ref:`discoext`.
 - Support for Python 2.6 and 2.7.
 - Easier installation of a simple single-server cluster. Just run ``disco
   master start`` on the disco directory. The ``DISCO_MASTER_PORT`` setting is deprecated.
 - Improved support for OS X. The ``DISCO_SLAVE_OS`` setting is deprecated.
 - Debian packages upgraded to use Erlang 13B.
 - Several improvements related to fault-tolerance of the system
 - Serialize job parameters using more efficient and compact binary format.
 - Improved support for GlusterFS (2.0.6 and newer).
 - Support for the pre-0.1 ``disco`` module, ``disco.job`` call etc., removed.

Bugfixes
''''''''

 - **critical** External sort didn't work correctly with non-numeric keys (5ef88ad4)
 - External sort didn't handle newlines correctly (61d6a597f)
 - Regression fixed in :meth:`disco.core.Disco.jobspec`; the function works now
   again (e5c20bbfec4)
 - Filter fixed on the web UI (bug #4, e9c265b)
 - Tracebacks are now shown correctly on the web UI (bug #3, ea26802ce)
 - Fixed negative number of maps on the web UI (bug #28, 5b23327 and 3e079b7)
 - The ``comm_curl`` module might return an insufficient number of bytes (761c28c4a)
 - Temporary node failure (noconnection) shouldn't be a fatal error (bug #22, ad95935)
 - *nr_maps* and *nr_reduces* limits were off by one (873d90a7)
 - Fixed a Javascript bug on the config table (11bb933)
 - Timeouts in starting a new worker shouldn't be fatal (f8dfcb94)
 - The connection pool in ``comm_httplib`` didn't work correctly (bug #30, 5c9d7a88e9)
 - Added timeouts to ``comm_curl`` to fix occasional issues with the connection
   getting stuck (2f79c698)
 - All `IOErrors` and `CommExceptions` are now non-fatal (f1d4a127c)


Disco 0.2.2 (July 26th 2009)
----------------------------

New features
''''''''''''

 - Experimental support for POSIX-compatible distributed filesystems, 
   in particular `GlusterFS <http://gluster.com>`_. Two modes are available: Disco
   can read input data from a distributed filesystem while preserving data locality
   (aka *inputfs*). Disco can also use a DFS for internal communication,
   replacing the need for node-specific web servers (aka *resultfs*).


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

