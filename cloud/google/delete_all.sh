#!/bin/sh
# Run this script only after you are done with the cluster and want to discard
# the whole cluster. It will delete all of the traces of the cluster created by
# setup.sh

if [ $# -ne 1 ]
then
    echo "Usage: ./delete_all n_slaves"
    exit 1
fi
N_SLAVES=$1

ZONE=us-central1-a
NODES="discomaster "
for i in $(seq $N_SLAVES)
do
    NODES=$(echo $NODES "disconode"$i)
done

echo "deleting the instances"
gcloud compute instances delete $NODES --zone $ZONE --quiet

echo "deleting the disco image"
gcloud compute images delete discoimage --quiet

echo "deleting the discoinstance disk"
gcloud compute disks delete discoinstance --zone $ZONE --quiet

echo "deleting the firewall rule"
gcloud compute firewall-rules delete http --quiet
