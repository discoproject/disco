Get Disco
=========

Latest release
--------------


Get one of the `official releases`_ and follow the :doc:`installation instructions <install>` after downloading the package.

Download the latest development version from `github`_::

        git clone git://github.com/tuulos/disco.git

Amazon EC2
----------

For a few dollars you can rent a server or a cluster of servers at
the Amazon's Elastic Computing Cloud (`EC2`_).
This is an easy way to get started with Disco as you don't have to install anything manually.
Download the `setup-instances.py`_ script and follow the :doc:`ec2setup` instructions.

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

.. warning:: **Our Debian packages are experimental!**
        They may not play nicely with other packages or they may destroy your computer.
        They are best to be used in a secure, sandboxed environment where nodes can be re-installed automatically if needed.
        If unsure, try first the Amazon EC2 option above which is based on our Debian packages.

.. _official releases: http://github.com/tuulos/disco/downloads
.. _github: http://github.com/tuulos/disco
.. _EC2: http://aws.amazon.com
.. _setup-instances.py: http://github.com/tuulos/disco/blob/master/aws/setup-instances.py
