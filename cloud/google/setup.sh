#!/bin/sh

# This is the main script for setting up the cluster. The user should run this script
# on the local machine which is authorized to use google cloud

if [ $# -ne 1 ]
then
    echo "Usage: ./setup.sh n_slaves"
    exit 1
fi
N_SLAVES=$1
ZONE=us-central1-a

echo "Creating a disco instance"
gcloud compute instances create discoinstance --image debian-7 --zone $ZONE --machine-type f1-micro || exit 1

echo "Waiting for the instance to boot up..."
sleep 20

gcloud compute firewall-rules create http --description "Disco HTTP Access" --allow tcp:8989

gcloud compute copy-files node_setup.sh discoinstance: --zone $ZONE || exit 2

echo "Installing disco on the instance"
gcloud compute ssh discoinstance --zone $ZONE --command "./node_setup.sh"
gcloud compute instances delete discoinstance --quiet --zone $ZONE --keep-disks boot

echo "Creating the disco image"
gcloud compute images create discoimage --source-disk discoinstance --source-disk-zone $ZONE

echo "Creating the cluster"
NODES="discomaster "
for i in $(seq $N_SLAVES)
do
    NODES=$(echo $NODES "disconode"$i)
done
gcloud compute instances create $NODES --image discoimage --machine-type f1-micro --zone $ZONE

echo "Waiting for the cluster to boot up..."
sleep 20
gcloud compute ssh discomaster --zone $ZONE --command "disco start"
