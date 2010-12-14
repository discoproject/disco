Get Disco
=========

Latest release
--------------


Get one of the `official releases`_ and follow the :doc:`installation instructions <install>` after downloading the package.

Development branches
--------------------

Clone a bleeding edge version from `github`_::

        git clone git://github.com/tuulos/disco.git

Debian packages
---------------

If you use Debian or a Debian-based distribution such as Ubuntu,
on the AMD64 architecture, you can just `apt-get install` Disco.
Add the following line to your `/etc/apt/sources.list`::

        deb http://discoproject.org/debian /

After running `apt-get update`, you can install the `disco-master` package to your master node.
If you run Disco in a cluster, you should install the `disco-node` package to other nodes.
The `python-disco` package is required on all machines where Disco scripts are run.

After installation, see steps 4-6 in :ref:`Setting up Disco <configauth>` that describe how to configure and test Disco.

Following distributions are supported

 - `python-disco` works on Debian stable (Lenny) and newer
 - `disco-master` and `disco-node` work on Debian testing (Squeeze) and newer
 - All packages should work on recent Ubuntu releases

This means that Debian stable can be used to submit Disco jobs but not to run
Disco master or nodes.

.. warning:: **Our Debian packages are experimental!**
        They may not play nicely with other packages or they may destroy your computer.

.. _official releases: http://github.com/tuulos/disco/downloads
.. _github: http://github.com/tuulos/disco
