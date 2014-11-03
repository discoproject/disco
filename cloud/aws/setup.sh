#!/bin/bash

if [ $# -ne 1 ]
then
    echo "Usage: ./setup.sh n_slaves"
    exit 1
fi

N_SLAVES=$1
KeyPair=""
SecurityGroupId=""
KEYFILE=""
REGION="us-west-2"
AWS="aws --region $REGION ec2"
INSTANCE_TYPE=t1.micro

# Create an instance from ubuntu 14.04 AMI
InstanceId=$($AWS run-instances --image-id 'ami-37501207' --count 1 --instance-type $INSTANCE_TYPE  --key-name "$KeyPair" --security-group-ids "$SecurityGroupId" --query 'Instances[0].InstanceId' | sed -e 's/^"//'  -e 's/"$//')
echo "instance_id is: " $InstanceId

Status=""

while [ -z "$Status" ] || [ "$Status" == "initializing" ]
do
    echo "Sleeping because status is: " $Status
    sleep 10
    Status=$($AWS describe-instance-status --instance-ids "$InstanceId" --query 'InstanceStatuses[0].SystemStatus.Status' | sed -e 's/^"//'  -e 's/"$//')
done

echo "Status is: " $Status " getting the ip address"

IP=$($AWS describe-instances --instance-ids $InstanceId --query 'Reservations[0].Instances[0].PublicIpAddress' | sed -e 's/^"//'  -e 's/"$//')
echo "ip is: ", $IP

scp -i $KEYFILE ../google/node_setup.sh ubuntu@$IP:
ssh -i $KEYFILE ubuntu@$IP "./node_setup.sh"

AMI=$($AWS create-image --instance-id $InstanceId --name discoimage --description "An Image for Creating Disco Intances" --query 'ImageId' | sed -e 's/^"//'  -e 's/"$//')
echo "AMI id is: ", $AMI
$AWS run-instances --image-id $AMI --count $N_SLAVES --instance-type $INSTANCE_TYPE --key-name "$KeyPair" --security-group-ids "$SecurityGroupId"

echo "The instances are ready and running, now allow incoming traffic from all of the nodes in the security group."
