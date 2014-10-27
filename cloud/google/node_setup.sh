#!/bin/sh
# This is a helper script that will be used to create the disco image.

SECRET_COOKIE=disco_secret

# Set up ssh
ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys

sudo sh -c "echo 'StrictHostKeyChecking no' >> /etc/ssh/ssh_config"


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
