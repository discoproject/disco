
.. _install_sys:

============================
Installing Disco System-Wide
============================

.. note:: **Shortcut for Debian / Ubuntu users:**
   If you run Debian or some recent version of Ubuntu on the AMD64 architecture,
   you may try out our *experimental* deb-packages which are available at the
   :doc:`Disco download page <download>`.
   If the packages installed properly, you should :ref:`configauth`.

Install From Source
===================

Assuming you have already gotten Disco running out of the source directory,
as described in :ref:`install_source`,
to install system-wide, just run ``make install`` as root::

        make install

This will build and install the Disco master to your system
(see the ``Makefile`` for exact directory locations).
You can specify ``DESTDIR`` and ``prefix``,
in compliance with `GNU make <http://www.gnu.org/software/make/manual/make.html>`_.

On systems that are intended to function as Disco worker nodes only,
you can use the ``make install-node`` target instead.

System Settings
===============

``make install`` installs a configuration file to ``/etc/disco/settings.py``
that is tuned for clusters, not a single machine.

By default,
the settings assume that you have at least three nodes in your cluster,
so DDFS can use three-way replication.
If you have fewer nodes,
you need to lower the number of replicas in ``/etc/disco/settings.py``::

        DDFS_TAG_MIN_REPLICAS=1
        DDFS_TAG_REPLICAS=1
        DDFS_BLOB_REPLICAS=1

Most likely you do not need to modify anything else in this file right now,
but you can change the settings here,
if the defaults are not suitable for your system.

See :mod:`disco.settings` for more information.

Creating a `disco` user
=========================

You can use any account for running Disco,
however it may be convenient to create a separate `disco` user.
Among other advantages,
this allows setting resource utilization limits for the `disco` user
(through ``limits.conf`` or similar mechanism).

Since Disco places no special requirements on the user,
(except access to certain ports and the ability to execute and read its files),
simply follow the guidelines of your system when it comes to creating new users.

Keeping Disco Running
=====================

You can easily integrate :mod:`disco <discocli>`
into your system's startup sequence.
As an example, you can see how ``disco-master.init``
is implemented in Disco's ``debian`` packaging.

Configuring DDFS Storage
========================

On the Disco nodes, DDFS creates by default a subdirectory named
``vol0`` under the :envvar:`DDFS_DATA` directory to use for storage.
If you have one or more dedicated disks or storage areas you wish to
use instead, you can mount them under the directory specified by
:envvar:`DDFS_DATA` as subdirectories named ``vol0``, ``vol1`` and so
on.
