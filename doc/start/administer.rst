
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

The job listing along the right side of the page shows running jobs in
yellow, completed jobs in green, and crashed or killed jobs in pink.
Hovering over a running job highlights the cores that the tasks of the
job are using on each node in light blue.

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
---------------------

Disco provides a tunable parameter ``max_failure_rate`` to
administrators to control the number of recoverable task failures a
job can experience before Disco fails the entire job.  This parameter
can be set on the configuration page.

.. _removenodes:

Removing nodes from Disco
-----------------------

You probably went through the process of adding a node to the cluster
when you configured the Disco cluster in :ref:`confignodes`.  Removing
a node is a very similar process, except that you need to click
``remove`` next to the node(s) you wish to remove, and then click
``save table`` when you are done.  If any DDFS tags or blobs were
hosted on the removed nodes, and the number of replicas for any of
tags or blobs falls below the configured minimum
(``DDFS_BLOB_REPLICAS``), then DDFS will start re-replicating them
from the existing replicas on the other nodes as described in
:ref:`ddfs`.  Note that it is never a good idea to remove more than
``DDFS_BLOB_REPLICAS` nodes at a time, since you may lose any data all
of whose replicas were hosted on those nodes.
