
.. _administer:

Administering Disco
===================

Monitoring the cluster
----------------------

An overall view of the state of the cluster is provided on the status
page, which is the default page for the web user interface, and is
accessible by clicking ``status`` on the top right of any page.  This
shows a node status box for each node in the cluster.

A black title background in the top row of the status box indicates
that the node is connected to the master, whereas a node that the
master is unable to connect to will have a crimson title background.
A blacklisted node will have a lavender background for the entire
status box.  Note that the master still attempts to connect to
blacklisted nodes, which means that the blacklist status and the
connection status for a node are independent.

The second row in the status box shows a box for each of the
configured cores on the node, and shows the busy cores in yellow.  The
third row shows amount of available disk space in the DDFS filesystem
on the node.

The fourth row shows some job statistics for the node.  The leftmost
number shows the number of tasks that successfully completed on the
node.  The middle number indicates the number of tasks that were
restarted due to them experiencing recoverable failures.  The
rightmost number indicates the number of tasks that crashed due to
non-recoverable errors.  Since a crashing task also causes its job to
fail, this number also indicates the number of jobs that failed due to
tasks crashing on this node.

The job listing along the top right side of the page shows running jobs in
yellow, completed jobs in green, and crashed or killed jobs in pink.
Hovering over a running job highlights the cores that the tasks of the
job are using on each node in light blue.

Below the job listing on the right is some information from the last
DDFS garbage collection run.  A table mentions the number of tags and
blobs that were kept after the last GC run and their total sizes in
bytes, along with similar information for deleted blobs and tags.

Blacklisting a node
-------------------

You may decide that you need to reduce the load on a node if it is not
performing well or for some other reason.  In this case, you can
blacklist a node, which informs Disco that the node should not be used
for running any tasks or storing any new DDFS data.  However, Disco
will still use the node for reading any DDFS data already stored on
that node.  Note that blacklisting a node will not trigger
re-replication of data away from the node.

Blacklisting a node is done on the configuration page, accessible by
clicking ``configure`` on the top right side of any page.  Type the
name of the node in the text entry box under ``Blacklisted nodes``,
and press enter.  The node should now show up above the text entry box
in the list of blacklisted nodes.  Any node in this list can be
clicked to whitelist it.

The set of blacklisted nodes is persisted, so that it is not lost
across a restart.

.. _adjustsettings:

Adjusting Disco system settings
-------------------------------

Disco provides a tunable parameter ``max_failure_rate`` to
administrators to control the number of recoverable task failures a
job can experience before Disco fails the entire job.  This parameter
can be set on the configuration page.

.. _removenodes:

Removing nodes from Disco
-------------------------

You probably went through the process of adding a node to the cluster
when you configured the Disco cluster in :ref:`confignodes`. Removing
a node from the Disco configuration is a very similar process, except
that you need to click ``remove`` next to the node(s) you wish to
remove, and then click ``save table`` when you are done.

There are a couple of options to removing a node that hosts DDFS data
from the Disco cluster.

   * The node could be physically removed from the cluster, but left
     in the Disco configuration.  In this case, it counts as a failed
     node, which reduces by one the number of additional node failures
     that could be safely tolerated.  :ref:`gcrr` will attempt the
     replication of any missing live blobs and tags.

   * The node could be physically removed from the cluster as well as
     the Disco configuration.  In this case, Disco treats the missing
     node as an unknown node instead of as a failed node, and the
     number of additional node failures tolerated by DDFS does not
     change.  However, this voids safety, since Disco might allow more
     nodes hosting DDFS data to fail than is safe.  If this happens,
     :ref:`gcrr` might not suffice to replicate the missing blobs and
     tags, and some data might be permanently lost.

The drawback of both of these approaches is that there is no
indication provided by Disco as to when, if ever, DDFS is in a
consistent state again with respect to the data that was hosted on the
removed node.

DDFS now allows scheduling the removal of a node from DDFS, by putting
the node on a DDFS *blacklist*.  If the node is alive but scheduled
for removal, the number of additional node failures that can be safely
tolerated does not change.  In addition, DDFS now provides an
indication when all the data and metadata that was hosted on that node
has been re-replicated on other cluster nodes, so that that node can
be safely removed from the Disco cluster with a guarantee that no data
has been lost.  The indication is provided by the node entry being
highlighted in green in the blacklist.
