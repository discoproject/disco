#!/bin/sh
# This is a helper script that will be used to create the disco image.

if [ $# -ne 1 ]
then
    echo "Usage: ./node_setup.sh n_slaves"
    exit 1
fi
N_SLAVES=$1
SECRET_COOKIE=disco_secret

# Set up ssh
ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

NODES="discomaster "
for i in $(seq $N_SLAVES)
do
    NODES=$(echo $NODES "disconode"$i)
done

for node in $NODES
do
    echo -n $node " ssh-rsa " >> ~/.ssh/known_hosts
    ssh-keyscan discoinstance | awk '{print $3}' >> ~/.ssh/known_hosts
done

# Install dependencies
sudo apt-get update
sudo apt-get -y install erlang git make

# Set up distributed Erlang
echo $SECRET_COOKIE >> cookie
mv cookie .erlang.cookie
chmod 400 .erlang.cookie

# Install disco
git clone https://github.com/discoproject/disco
cd disco
git checkout origin/master
make
sudo make install
cd lib
sudo python setup.py install
sudo chown -R $USER /usr/var/disco
