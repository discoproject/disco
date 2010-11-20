
.. _administer:

Administering Disco
===================

Once a cluster is up and running, the most common administration tasks
are monitoring the health of the cluster, blacklisting a node in the
cluster, adjusting Disco system settings, and adding a node to and
removing a node from the cluster.

Monitoring the cluster
----------------------

An overall view of the state of the cluster is provided on the status
page.  This shows for each node in the cluster, whether the node is
connected to the master, whether the node was blacklisted, whether the
cores on the node are busy running tasks, the amount of available disk
space for DDFS data on that node, and statistics on how many tasks on
that node succeeded, failed, or crashed.

Blacklisting a node
-------------------

You may decide that you need to reduce the load on a node in the
cluster, for e.g., if it does not perform well under load.  In this
case, you can blacklist a node, which informs Disco that the node
should not be used for running any tasks or storing any new DDFS data.
However, Disco will still use the node for reading any DDFS data
already stored on that node.  Note that blacklisting a node will not
trigger Disco to re-replicate data away from the node.

Blacklisting a node is done on the configuration page, accessible by
clicking ``configure`` on the right side of the page.  Type the name
of the node in the text entry box under ``Blacklisted nodes``, and
press enter.  The node should now show up in the list of blacklisted
nodes.  Any node in this list can be clicked to whitelist it.

The set of blacklisted nodes is persisted, so that it is not lost
across a restart.

.. _adjustsettings:

Disco System Settings
---------------------

Disco provides a tunable parameter ``max_failure_rate`` to
administrators to control the number of task failures a job can
experience before Disco fails the job itself.  This parameter can be
set on the configuration page.

.. _removenodes:

Remove nodes from Disco
-----------------------

You probably went through the process of adding a node to the cluster
when you configured the Disco cluster in :ref:`confignodes`.  Removing
a node is a very similar process, except that you need click
``remove`` next to the node(s) you wish to remove, and then click
``save table`` when you are done.  If any DDFS tags or blobs were
hosted on the removed nodes, and the number of replicas for any of
tags or blobs falls below the configured minimum
(``DDFS_BLOB_REPLICAS``), then DDFS will start re-replicating them
from the existing replicas on the other nodes as described in
:ref:`ddfs`.  Note that it is never a good idea to remove more than
``DDFS_BLOB_REPLICAS` nodes at a time, since you may lose any data all
of whose replicas were hosted on those nodes.
