
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

.. _proxy_config:

Using a proxy
-------------

A cluster is sometimes configured with a few head nodes visible to its
users, but most of the worker nodes hidden behind a firewall.  Disco
can be configured to work in this environment using a HTTP proxy,
provided the node running the Disco master is configured as one of the
head nodes, and the port used by the proxy is accessible to the Disco
users.

To use this mode, the Disco settings on the master (in the
``/etc/disco/settings.py`` file) should set ``DISCO_PROXY_ENABLED`` to
``"True"``.  Disco then starts a HTTP proxy specified by
``DISCO_HTTPD`` (defaulting to `lighttpd`) on port
``DISCO_PROXY_PORT`` (defaulting to ``8999``) with a configuration
that proxies HTTP access to the Disco worker nodes.  Currently, Disco
supports generating configurations for the `lighttpd` and `varnish`
proxies.  The Disco master needs to be restarted for any changes in
these settings to take effect.

To use this proxy from the client side, i.e. in order to use the
``disco`` and ``ddfs`` commands, the users need to set ``DISCO_PROXY``
in their environment, or in their local ``/etc/disco/settings.py``
file.  The value of this should be in the format
``http://<proxy-host>:<proxy-port>``, with ``<proxy-host>`` and
``<proxy-port>`` specifying how the proxy is accessible to the user.

.. _discoblacklist:

Blacklisting a Disco node
-------------------------

You may decide that you need to reduce the load on a node if it is not
performing well or for some other reason.  In this case, you can
blacklist a node, which informs Disco that the node should not be used
for running any tasks or storing any new DDFS data.  However, Disco
will still use the node for reading any DDFS data already stored on
that node.  Note that blacklisting a node will not trigger
re-replication of data away from the node (however, see below on
blacklisting a DDFS node).

Blacklisting a node is done on the configuration page, accessible by
clicking ``configure`` on the top right side of any page.  Type the
name of the node in the text entry box under ``Blacklisted nodes for Disco``,
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


.. _ddfsblacklist:

Blacklisting a DDFS node
------------------------

There are various ways of removing a node that hosts DDFS data from
the Disco cluster, with differing implications for safety and data
availability.

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
     nodes hosting DDFS data to fail than is safe.  For example, if
     the replication factor is set to 3, Disco might treat two
     additional node failures as safe, whereas actually those two
     nodes might be hosting the last two remaining replicas of a blob,
     the third replica being lost when the first node was removed from
     the configuration.  If this happens, :ref:`gcrr` will not suffice
     to replicate the missing blobs and tags, and some data might be
     permanently lost.

The drawback of both of these approaches is that there is no
indication provided by Disco as to when, if ever, DDFS is in a
consistent state again with respect to the data that was hosted on the
removed node.

DDFS now allows scheduling the removal of a node from DDFS, by putting
the node on a DDFS *blacklist*, which is specified using the text
entry box labeled ``Blacklisted nodes for DDFS``.  This makes
:ref:`gcrr` actively replicate data away from that node; that is,
additional replicas are created to replace the blobs hosted on a
blacklisted node, and when safe, references to the blobs on that node
are removed from any referring tags.  DDFS data on the blacklisted
node is however not deleted.

In addition, DDFS now provides an indication when all the data and
metadata that was hosted on that node has been re-replicated on other
cluster nodes, so that that node can be safely removed from the Disco
cluster (both physically, as well as from the configuration) with a
guarantee that no data has been lost.  The indication is provided by
the node entry being highlighted in green in the blacklist.  It may
require several runs of :ref:`gcrr` to re-replicate data away from a
node; since by default it runs once a day, several days may be needed
before a DDFS blacklisted node becomes safe for removal.

.. _master_recovery:

Handling a master failure
-------------------------

Disco is currently a single master system, which means it has a single
point of failure.  This master controls both job scheduling as well as
the :ref:`ddfs`.  The failure of the master will result in the
termination of currently running jobs and loss of access to the
:ref:`ddfs`.  However, it will not result in any loss of data in DDFS,
since all metadata in DDFS is replicated, just like data.  The only
centralized static information is the Disco settings file on the
master (specified by the ``DISCO_SETTINGS_FILE``, which defaults to
``/etc/disco/settings.py`` for installation), and the Disco cluster
configuration, maintained in the file specified by the
``DISCO_MASTER_CONFIG`` setting.  You can examine all the settings for
Disco using the ``disco -v`` command.

A failed Disco master can be replaced by installing the Disco master
on a new machine (or even an existing Disco node, though this is not
recommended for large or busy clusters).  See :ref:`_install_sys` for
details on installing the Disco master.  On the replacement machine,
you will need to copy the settings and configuration files from the
original master into their expected locations.  For this reason, it is
a good idea to backup these files after any change.  The
``DISCO_SETTINGS_FILE`` is manually modified, while the
``DISCO_MASTER_CONFIG`` file is managed by the Disco master.  The
config file is changed whenever nodes are added or removed as members
of the cluster, or any blacklist.
