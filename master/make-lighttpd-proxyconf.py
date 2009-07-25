#!/usr/bin/python
import os, re

port = os.environ["DISCO_PORT"]
r = re.compile("^(\d+\.\d+\.\d+\.\d+)\s+(.*)", re.MULTILINE)
print "proxy.server = ("
for x in re.finditer(r, file("/etc/hosts").read()):
        ip, host = x.groups()
        print '"/disco/node/%s/" => (("host" => "%s", "port" => %s)),' %\
                (host, ip, port)
print ")"
