
:mod:`disco.dfs.gluster` --- GlusterFS interface
================================================

.. module:: disco.dfs.gluster
   :synopsis: GlusterFS interface

This module provides functions that help using GlusterFS with Disco.
For more information on how to setup GlusterFS for Disco, see :ref:`dfs`.

Note that you can use Gluster with Disco without this module. Functions
in this module are provided only for convenience.

This module requires the `xattr module <http://pypi.python.org/pypi/xattr>`_
(``python-xattr`` in Debian / Ubuntu) which is used to resolve nodes hosting
replicas on GlusterFS.

.. function:: files(path[, filter, hostname_map])

   This function walks recursively through a specified *path* of *inputfs* (often
   *path* refers to a directory that contains one dataset) and constructs a list 
   of input addresses.

   ``files()`` supports replication. It uses the ``xattr`` module (see above) to
   resolve nodes that host replicas of each file based on the file's extended
   attributes. A valid `dfs://` address returned for each replica, so Disco can
   use them in case that the disk hosting the primary copy fails. This means
   that ``files()`` returns a list of lists.
   
   Only file names that match *filter* are included in the list. *filter* can be
   either a substring occurring anywhere in the file's absolute path or a compiled 
   `Regular Expression object <http://docs.python.org/library/re.html>`_ which is 
   matched against the filename.

   *hostname_map* is a dictionary that maps GlusterFS (*inputfs*) volume names to
   node names used by Disco. Often volume names are different from names of
   Disco nodes, although the physical servers are the same. For instance, a
   volume might be called ``nx02-vol1`` whereas the same disco node is called
   ``disco-02``. *hostname_map* provides a mapping between the two namespaces.
   The mapping is also useful if your Disco nodes run in virtual machines and 
   GlusterFS on physical nodes that host the virtual machines.

   If you don't want to provide a mapping, you can specify ``hostname_map =
   {}``. In this case volume names are used as such in returned addresses.
   Your job will finish successfully even with incorrect hostnames but the 
   scheduler is unable to schedule computation near data, thus the job
   is likely to run slower. 

   There are three ways to provide *hostname_map*:

    * In a dictionary which keys are volume names and values disco node names.
    
    * If ``hostname_map = None`` (default), the mapping is read from a file at
      ``/inputfs/.hostname_map`` where ``/inputfs/`` is the root of your 
      *inputfs* filesystem. The mount point is inferred automatically 
      based on *path*. The file format is the following::

        [gluster-volume-01] [disco-node01]
        [gluster-volume-02] [disco-node02]
        ...

    * If ``hostname_map = None`` and the ``DISCO_HOSTNAME_MAP`` environment 
      variable is specified, the mapping is read from a file specified in
      ``DISCO_HOSTNAME_MAP``. The file follows the format specified above.

   Here's a typical example of ``files()``::

        from disco.dfs.gluster import files

        Disco(master).new_job(
                name = "web_log_analysis",
                input = files("/data/web_logs/", "2006-07"),
                map = my_map)

   This example reads recursively all files that contain the substring
   ``2006-07``  under the ``/data/web_logs`` directory that should 
   be part of the *inputfs* hierarchy. Since ``files()`` returns all *K* 
   replicas of each input file, execution of the map function ``my_map`` 
   can continue even if *K - 1* disks fail.



