#!/usr/bin/python
import os, re

def resultfs_config():
    print 'url.rewrite = ( "^/disco/node/(.*?)/(.*)" => /disco/master/$2" )'

def normal_config():
    port = os.environ['DISCO_PORT']
    ip_re = re.compile(r'^(\d+\.\d+\.\d+\.\d+)')
    print 'proxy.server = ('
    for line in open('/etc/hosts'):
        if ip_re.match(line):
            pieces = line.split()
            ip = pieces[0]
            for host in pieces[1:]:
                print '"/disco/node/%s/" => (("host" => "%s", "port" => %s)),' % (host, ip, port)
    print ')'
    print 'url.rewrite-once = ( "/disco/node/localhost/(.*)" => "/$1" )'

if 'resultfs' in os.getenv('DISCO_FLAGS', '').lower().split():
    resultfs_config()
else:
    normal_config()

