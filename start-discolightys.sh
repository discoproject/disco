#!/bin/bash

for i in `cat $1`
do
        scp lighttpd-disco.conf $i:
        ssh $i "/usr/sbin/lighttpd -f lighttpd-disco.conf -D &"
        echo "Lighty started at $i"
done
