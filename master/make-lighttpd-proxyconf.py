#!/usr/bin/python
import os, re

port = os.environ["DISCO_PORT"]
print "proxy.server = ("
for x in re.finditer("(\d+\.\d+\.\d+\.\d+)\s+(.*)", file("/etc/hosts").read()):
        ip, host = x.groups()
        print '"/disco/node/%s/" => (("host" => "%s", "port" => %s)),' %\
                (host, ip, port)
print ")"
