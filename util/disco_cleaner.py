#
# Usage:
#
# python disco_cleaner.py http://cfront:5000 /var/disco /scratch/cnodes
#

import sys, os, urllib, time, datetime, re
from subprocess import *
from itertools import *
from operator import itemgetter

SSH = ["ssh", "-o", "ConnectTimeout=1"]
REMOTE_JOBS = ["ls --time-style=long-iso -lH " + sys.argv[2]]
DELETE_JOB = ["rm", "-Rf"]

DISCO_RESULTS = "/disco/ctrl/get_results"

def all_nodes():
        for x in file(sys.argv[3]):
                yield x.strip()

def job_entry_exists(name):
        url = sys.argv[1].replace("disco:", "http:", 1)\
                + DISCO_RESULTS + "?name=" + name
        R = urllib.urlopen(url).read()
        return eval(R)[0].lower() != "unknown job"

# Collect job names and timestamps from nodes
dirs = []
for node in all_nodes():
        p = Popen(SSH + [node.strip()] + REMOTE_JOBS, stdout = PIPE)
        for dir in p.stdout:
                try:
                        date0, time0, name = dir.split()[-3:]
                except ValueError:
                        continue

                if "@" not in dir or re.search("\W", name.replace("@","")):
                        print >> sys.stderr, "Skipping", name
                        continue

                tstamp = time.strptime("%s %s" % (date0, time0),\
                        "%Y-%m-%d %H:%M")
                tstamp = datetime.datetime(*tstamp[:6])
                dirs.append((name, tstamp))

# Filter entries that don't have a job entry and are older than 24h
kill_list = []
nu = datetime.datetime.today()
dirs.sort()
for dir in groupby(dirs, key = itemgetter(0)):
        if job_entry_exists(dir[0]):
                continue
        tstamps = [t for n, t in dir[1]]
        if nu - max(tstamps) > datetime.timedelta(days = 1):
                kill_list.append(dir[0])

# Delete old job data
for target in kill_list:
        target = sys.argv[2] + "/" + target
        print >> sys.stderr, "Deleting", target
        procs = []
        for node in all_nodes():
                procs.append((node,\
                        Popen(SSH + [node] + DELETE_JOB + [target])))
        for name, proc in procs:
                if proc.wait():
                        print >> sys.stderr,\
                                "Couldn't delete %s:%s" % (name, target)



