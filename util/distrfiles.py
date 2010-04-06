import os
import sys
import math
import random

from subprocess import Popen, call, PIPE


def log(s):
    print >> sys.stderr, s


USAGE_INFO = """
Usage: distrfiles.py <dataset-path> <nodes-file>\n
DISCO_ROOT must specify the disco home directory (e.g. /srv/disco/).
SSH_KEY can specify a value for the -i flag of ssh / scp.
SSH_USER can specify the user name.
REMOVE_FIRST if set, removes previous dataset of the same name before copying.
"""


if len(sys.argv) < 3:
    log(USAGE_INFO)
    sys.exit(1)

if "DISCO_ROOT" not in os.environ:
    log("Specify DISCO_ROOT (data lives at $DISCO_ROOT/data)")
    sys.exit(1)

root = os.environ["DISCO_ROOT"]
ssh = []
if "SSH_KEY" in os.environ:
    ssh += ["-i", os.environ["SSH_KEY"]]

user = os.environ.get("SSH_USER", "")

files = os.listdir(sys.argv[1])
if sys.argv[1][-1] == "/":
    name = os.path.basename(sys.argv[1][:-1])
else:
    name = os.path.basename(sys.argv[1])

# check to see if ionice is installed on this system
status = Popen('which ionice'.split(), stdout=PIPE).wait()
if status == 0:
    ionice = 'ionice -n 7'
else:
    ionice = ''

nodes = []
hostname = {}
for l in file(sys.argv[2]):
    s = l.strip().split()
    if len(s) > 1:
        hostname[s[0]] = s[1]
    else:
        hostname[s[0]] = s[0]
    nodes.append(s[0])

fno = int(math.ceil(max(1, float(len(files)) / len(nodes))))

log("Dataset name: %s" % name)
log("%d files and %d nodes: %d files per node" % (len(files), len(nodes), fno))

random.shuffle(nodes)
nodes = iter(nodes)
procs = []

while True:
    nset = [sys.argv[1] + "/" + f  for f  in files[:fno]]
    if not nset:
        break
    files = files[fno:]
    node = unode = nodes.next().strip()
    if user:
        unode = "%s@%s" % (user, node)

    if "REMOVE_FIRST" in os.environ:
        call(["ssh"] + ssh + [unode, "rm -Rf %s/data/%s" % (root, name)])

    if call(["ssh"] + ssh + [unode, "mkdir /%s/data/%s" % (root, name)]):
        log("Couldn't mkdir %s:%s/data/%s" % (node, root, name))

    log("Copying %d files to %s" % (len(nset), node))
    p = Popen("nice -19 %s scp %s %s %s:%s/data/%s/" %
        (ionice, " ".join(ssh), " ".join(nset), unode, root, name),
            shell = True)
    procs.append((node, p, nset))

log("Waiting for copyers to finish..")

for node, p, nset in procs:
    if p.wait():
        log("Copying to %s failed" % node)
    else:
        log("%s ok" % node)
        print "\n".join(("disco://%s/%s/%s" %
            (hostname[node], name, os.path.basename(f))
                for f in nset))

log("Done")

