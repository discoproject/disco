#!/usr/bin/python
import os, re


def resultfs_config():
        print 'url.rewrite = ( "^/disco/node/(.*?)/(.*)" => '\
              '"/disco/master/$2" )'

def normal_config():
        port = os.environ["DISCO_PORT"]
        r = re.compile("^(\d+\.\d+\.\d+\.\d+)\s+(.*)", re.MULTILINE)
        print "proxy.server = ("
        for x in re.finditer(r, file("/etc/hosts").read()):
                ip, host = x.groups()
                print '"/disco/node/%s/" => '\
                      '"(("host" => "%s", "port" => %s)),' %\
                                (host, ip, port)
        print ")"

if "resultfs" in os.environ.get("DISCO_FLAGS", "").lower().split():
        resultfs_config()
else:
        normal_config()


