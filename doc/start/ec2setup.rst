
.. _ec2setup:

How to set up Disco on Amazon EC2
=================================

With the following three steps, you can set up
a Disco cluster in the `Amazon's Elastic Computing Cloud
<http://www.amazon.com/EC2-AWS-Service-Pricing/b/ref=sc_fe_l_2?ie=UTF8&node=201590011>`_.
This will cost you a few dollars (or more, depending on your needs)
but requires no resources on your side besides a single machine that
you use to setup the cluster.

In this setup Disco master and nodes run on EC2. Your Disco client can
run either on the master node on EC2 or on a local machine.

1. Sign up for Amazon EC2
-------------------------

`Amazon EC2 Getting Started Guide
<http://docs.amazonwebservices.com/AWSEC2/2008-05-05/GettingStartedGuide/>`_
will help you in this step. Keep it at hand.

You need an Amazon Web Services account
to use EC2. Go to the `Amazon Elastic Compute Cloud
<http://www.amazon.com/EC2-AWS-Service-Pricing/b/ref=sc_fe_l_2?ie=UTF8&node=201590011>`_
home page and click "Sign up for this web service". See the section
`"Setting up an account"
<http://docs.amazonwebservices.com/AWSEC2/2008-05-05/GettingStartedGuide/index.html?account.html>`_ in the Getting Started Guide and follow the given instructions.

Once you have an account, `download EC2 command-line tools
<http://developer.amazonwebservices.com/connect/entry.jspa?externalID=351&categoryID=88>`_
that are used to communicate with EC2. You need
Java Runtime Environment (version 5 or newer) to run the
tools.  Follow the instructions in the section `"Setting up the tools"
<http://docs.amazonwebservices.com/AWSEC2/2008-05-05/GettingStartedGuide/index.html?setting-up-your-tools.html>`_
and set up the necessary environment variables (EC2_HOME, EC2_PRIVATE_KEY,
EC2_CERT, and JAVA_HOME).

2. Verify that your account works
---------------------------------

You need a private SSH keypair that is used to control
server instances in EC2 securely. Follow the instructions
in the `"Running an instance / Generating an SSH keypair" section
<http://docs.amazonwebservices.com/AWSEC2/2008-05-05/GettingStartedGuide/index.html?running-an-instance.html>`_
to create a keypair. In the end, you should have a file that contains
your private key.

Now you can try to start a server instance. Run the following command::

        ec2-run-instances -t m1.large -k your-key-file ami-e69d798f

This requests a new server instance from EC2. Here the ``-t`` flag
specifies the instance type -- we want a 64-bit one and choose
``m1.large``. You can check the available types from the `EC2 FAQ
<http://www.amazon.com/FAQ-EC2-AWS/b?ie=UTF8&node=201591011>`_.

The ``-k`` flag specifies your keypair file which was generated above.

The last argument, ``ami-e69d798f``, specifies the server image (operating
system) that we want to use. We use a pre-installed `Debian 5.0 Lenny
by Alestic.com <http://alestic.com/>`_.

It takes a while for the instance to start up. You can see the status of
running instances as follows::

        ec2-describe-instances

Once the instance has started up, you should see an address like
``ec2-75-101-194-126.compute-1.amazonaws.com`` somewhere on the output.

Once the instance is running, you can log in to it as follows::

        ssh -i your-key-file -l root ec2-address

where ``-i`` specifies your keypair. Replace ``ec2-address`` with the 
address returned by ``ec2-describe-instances``.

If everything is ok, you should have logged in to the server which looks
and feels like a normal bare-bones Linux server. There's nothing you
need to do on the server as a scripts will set up Disco automatically
in the following step.

Note that a running instance costs you money every hour! You can terminate
it by saying::

        ec2-terminate-instance i-454e2b22

where ``i-454e2b22`` is an instance ID as returned by
``ec2-describe-instances`` above. You can see how much money you have
spent this far on your "AWS Account Activity" page at Amazon.

If you can't log in to the server, see the EC2 Getting Started Guide
and EC2 forums for help.


3. Set up Disco
---------------

Now that you can succesfully start server instances on Amazon EC2,
you are ready to launch a Disco cluster.

First, we need server instances for the cluster. You can start as many
instances as you like (and can afford) but you need at least two --
a master and a node. Note that the setup script used below will never
start or stop any instances automatically. It is up to you to decide
how much money to spend.

Start the instances as above::
        
        ec2-run-instances -n 5 -t m1.large -k your-key-file ami-e69d798f

Here the ``-n`` flag specifies the number of instances. Wait until
``ec2-describe-instances`` shows five instances with the ``running``
status and valid addresses.

Go to the :doc:`Disco download page <download>`
and download the latest ``setup-instances-XX.py`` script. The script
will find the running instances using ``ec2-describe-instances`` and it
will install the necessary packages to the nodes. Run it as follows::

        python setup-instances.py your-key-file

The script will run for several minutes while it downloads and installs
packages to the nodes.

If the script was succesful, you should see a message saying ``All done!``
which means that the Disco cluster is running on EC2! Now you can connect
to the Disco master by pointing your browser at ``http://localhost:8989``.
You should see the Disco main screen and some boxes depicting available
nodes. Communication to the Disco master takes place over a secure SSH
tunnel which is started at port 8989. You can open more tunnels to the
master node as necessary using your SSH keypair.

If the script wasn't succesful, you can try to re-run it with the
``VERBOSE=1`` environment variable set. This will make the script to
redirect outputs of all commands it runs to the standard output which
might reveal what goes wrong.

Remember to terminate your cluster when you are done with it. You can easily
start up a new one when needed just by repeating this step.

4. Test the system
------------------

Now you are ready to run a Disco script in your new EC2 cluster. We can
use the script provided in the installation guide for local clusters,
in section :ref:`insttest`. Copy the given script in a file, and run it
as follows::

        DISCO_PROXY=http://localhost:8989 python2.4 count_words.py http://localhost:8989

You can run the script either on your local machine, through the SSH tunnel that
was created by ``setup-instances`` or on the master node of your EC2 cluster.

If the script prints out a list of word frequencies, and you can see a job in
the Disco's Web UI at http://localhost:8989, congratulations, you have a working
Disco cluster!


Using Disco on EC2
------------------

In general, you can use the EC2 cluster as any other Disco
cluster. However, note the following differences.

You need to set the ``DISCO_PROXY=http://localhost:8989`` environment
variable when running any Disco jobs using EC2. This is required to access
result files using the master node as a proxy, as the computation nodes
on EC2 are not directly accessible.

The ``setup-instances.py`` script produces a file called ``ec2-nodes``
that maps node names to their addresses on EC2. You need to provide
this file to ``util/distrfiles.py`` when distributing data to your EC2
cluster. You have to specify your SSH keypair to the script as well.

Here's an example::

        export DISCO_ROOT=/srv/disco/
        export SSH_KEY=your-key-file
        export SSH_USER=root
        python util/distrfiles.py your-data-directory ec2-nodes > my.chunks

The resulting ``my.chunks`` file can be used as usual to define inputs
for Disco jobs. See the :ref:`tutorial` to see how this works in practice.





