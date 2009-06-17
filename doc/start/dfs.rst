
.. _dfs:

Using Disco with a distributed filesystem
=========================================

.. contents::

Disco does not include a built-in storage layer. Disco
can use any POSIX-compatible local filesystem for storage and any HTTP
server for communication. This makes it possible to integrate Disco
easily to existing infrastructure and to choose the filesystem which
works well with expected workloads.

Since version 0.2.2, Disco supports also distributed filesystems
(DFS) that provide POSIX-semantics, that is, distributed filesystems
that look and feel like ordinary local filesystems and which can be mounted 
to your local file hierarchy. See `List of distributed filesystems in Wikipedia 
<http://en.wikipedia.org/wiki/List_of_file_systems#Distributed_file_systems>`_
for alternatives. 
        
Most often the right solution is a *distributed parallel filesystem* 
which distributes IO load over several nodes. The job scheduler in 
Disco is specifically optimized to preserve data locality in
cases like this. In contrast, centralized file servers
like NFS or CIFS do not distribute IO load to several nodes, so it seldom 
makes sense to use them with Disco. Disco can also benefit from 
*distributed parallel fault-tolerant filesystems* which replicate
files to several nodes. In this case, the job scheduler can choose
the instance of an input file which is located on the node with 
least load.

There are two independent use cases for a DFS: It can be used as a
storage for input data, or as a way to store and transfer intermediate
results in a Disco cluster. We call the former case *inputfs* and the
latter *resultfs*. You can choose to use DFS for either one of the use
cases or both. In practice, *inputfs* and *resultfs* are just shared
directories under the disco file hierarchy. See the sections below 
for more detailed descriptions.

Although you can use any POSIX-compatible DFS, Disco has been
mostly tested with a distributed parallel fault-tolerant filesystem
called `GlusterFS <http://gluster.org>`_. GlusterFS provides a
DFS layer on top of local filesystems in userspace, using `FUSE
<http://fuse.sourceforge.net/>`_, unlike traditional filesystems
which need to be included in the kernel. GlusterFS is remarkably easy
to set up: It requires only one server process running on each
node. A utility script, ``util/gluster_config.py`` is included in the
Disco sources, which can be used to configure a GlusterFS overlay for
*inputfs* and *resultfs*.

See :ref:`dfssetup` below for general instructions on how to setup the DFS
support for Disco. The section :ref:`gluster` gives specific instructions
for GlusterFS. 

Inputfs
-------

If your input data is remote, stored somewhere in the cloud, HTTP is an
obvious way to access the data in Disco. If your data is local, there
are more alternatives. The easiest way is to use local disks and web
servers together with a simple script, such as ``distrfiles``, which
distributes data to the cluster as depicted in :ref:`overview`. 

If your data is already stored in a suitable DFS, it is often a good
idea to configure Disco access data directly in the existing system. 
Whether it makes sense to install a DFS instead of using independent
local disks depends on your exact needs. The main benefits compared to 
a simple mechanism based on local disks include

 * Transparent file distribution (no need for separate scripts),
 * Transparent file replication for high-availability and performance,
 * Easier data management with a global namespace.

Downsides include added complexity and worse scalability -- local 
disks share nothing, so they are optimally scalable. Typically a DFS
is a good solution for adhoc data processing whereas a custom mechanism
works better in well-defined application domains. For an example of a
custom mechanism, see `Ringo <http://github.com/tuulos/ringo/tree/master>`_
which is an experimental, distributed key-value storage that supports
Disco natively.

Resultfs
--------

As an alternative to local web servers, *resultfs* provides a mechanism
to transfer intermediate results and other internal data within the
Disco cluster. *Resultfs* is 100% transparent to the user. There are no
user-visible differences compared to the default HTTP-based setup. 

The main benefits compared to the default setup are

 * Easier maintainability as local web servers are not needed; especially if used together with *inputfs*,
 * Possibly better performance, depending on the chosen DFS.

If there is a DFS already set up for *inputfs*, an HTTP-based solution
would be largely redundant. Also, it should be easier to tune a DFS
for maximum IO performance than a web server. Downsides include worse
scalability as with *inputfs* and in some cases worse performance,
if the chosen DFS is not suitable for the actual workloads or it is
configured improperly.

Note that *inputfs* and *resultfs* often require two separately
configured DFSs. *Inputfs* is often configured to minimize chances of
losing data by replicating data aggressively. With *resultfs* redundancy
is not as crucial since Disco can handle many IO errors internally.
On the other hand, it makes sense for *resultfs* to avoid moving data
unnecessarily and prefer local disks over remote ones whenever possible.
The GlusterFS configuration produced by the ``gluster_config.py`` script
is based on these assumptions.

.. _dfssetup:

Setting up a distributed filesystem for Disco
---------------------------------------------

Setting up a distributed filesystem for Disco is remarkably straightforward
once you have your DFS already up and running. If you don't, set it up
first following the documentation of your DFS or consult the next section about
GlusterFS.

.. _inputfs:

Inputfs
'''''''

*Inputfs* is a shared directory that is mounted at ``$DISCO_ROOT/input``. You
can mount it manually on every node of the disco cluster as follows::
        
        mkdir $DISCO_ROOT/input
        mount your-dfs-parameters $DISCO_ROOT/input

You should replace ``your-dfs-parameters`` with the parameters specific to your
DFS. You can also add a corresponding line to ``/etc/fstab`` to mount the
directory automatically when the node starts. Disco never writes or modifies
anything on *inputfs* so you can mount it read-only.

*Inputfs* is used with the ``dfs://`` protocol for input files (see *input* in
:meth:`disco.core.Disco.new_job`). For instance, the following address

``dfs://node06/weblogs/day-2009-06-16``

refers to a file at ``$DISCO_ROOT/input/weblogs/day-2009-06-16``. Although you
could use the absolute path to specify an input file, the ``dfs://`` protocol
hints Disco about the node where the file is physically stored thus allowing
the job scheduler to optimize data locality and minimize network traffic.

The host name is just a hint for Disco and the scheduler may choose to
assign a task that accesses the file on another node as well. Thus *inputfs* 
must provide an equal, shared view to files at ``$DISCO_ROOT/input`` on 
all nodes. 

Your DFS should provide a mechanism to find out where a file is physically
stored, so you can construct ``dfs://`` addresses automatically. Disco comes
with a Python module that can construct the addresses for GlusterFS.

Resultfs
''''''''

*Resultfs* is used to transfer intermediate results between the map and 
reduce phases. It requires two directories: a working directory on a local disk 
for the results of map tasks at ``$DISCO_ROOT/temp`` and a shared directory 
for accessing the results remotely during the reduce phase at
``$DISCO_ROOT/data``.

You can create a local directory for temporary results and mount your DFS to the 
``data``-directory manually as follows

::
        
        mount your-dfs-parameters $DISCO_ROOT/data
        mkdir $DISCO_ROOT/temp

As with *inputfs*, you can add the mount command to your ``/etc/fstab``. Disco
needs read and write access to both the directories. Enable *resultfs* 
by adding the flag ``resultfs`` to ``DISCO_FLAGS`` (i.e. ``DISCO_FLAGS=resultfs``) in 
your ``disco.conf``.

*Resultfs* works by writing the results and auxiliary files of a map task on a
local disk at ``$DISCO_ROOT/temp`` -- handling working data on a DFS would cause 
unnecessary overhead. Once the phase has finished succesfully, Disco moves the
results from ``$DISCO_ROOT/temp`` to ``$DISCO_ROOT/data`` so that they can be
accessed from all nodes.


















.. _gluster:

Configuring GlusterFS
---------------------
  
Setting up Gluster



