Set up a Disco Cluster in AWS (Alpha)
======================================


This script lets you create a Disco AMI and then use it to create a cluster of nodes.
It depends on the aws cli.
You still have to create an ssh key and a security group that allows all incoming traffic
to all of the ports of the nodes in the cluster.

Of course, you can do all of these tasks manually from the web interface. The bulk of
the work is done in the setup_node.sh script. Once the cluster is set up, make sure
to add the nodes to the config file.

You have to modify the script and add your KeyPair, SecurityGroup and KeyFile to it so that they can
be used when using the aws cli.
