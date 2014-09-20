Set up a Disco Cluster in Google Cloud
======================================

You can use disco in the Cloud! The scripts in this directory automate the
deployment of Disco on Google Cloud. You can simply run the setup script and
give it the number of worker nodes you want the cluster to have and everything
will be set up in a couple of minutes.

You can then go to discomaster:8989 and set the workers
to the following:

    Nodes:          disconode1:3
    Max Workers:    1

Which means we have three nodes and each node will have 1 workers.

The stuff that you should to modify in the scripts for use in production:

1. The type of the instances. This script uses micro instances which are perfect
   for testing.

2. The Erlang Cookie. Set it to something unique in the sript.

3. Add more firewall rules for your use-case.

Finally, when you are done with testing and want to discard the cluster, run the delete_all
script. You probably want to double check the console to make sure nothing is running afterwards.

