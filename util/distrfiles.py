import os, sys, math, random, os
from subprocess import *

if len(sys.argv) < 3:
    print >> sys.stderr, "\nUsage: distrfiles.py <dataset-path> <nodes-file>\n"
    print >> sys.stderr, "DISCO_ROOT must specify the disco home directory (e.g. /srv/disco/)."
    print >> sys.stderr, "SSH_KEY can specify a value for the -i flag of ssh / scp."
    print >> sys.stderr, "SSH_USER can specify the user name."
    print >> sys.stderr, "REMOVE_FIRST if set, removes previous dataset of the "\
                 "same name before copying.\n"
    sys.exit(1)

if "DISCO_ROOT" not in os.environ:
    print >> sys.stderr, "Specify DISCO_ROOT "\
        "(data lives at $DISCO_ROOT/data)"
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
status = Popen('which ionice'.split()).wait()
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

print >> sys.stderr, "Dataset name: %s" % name
print >> sys.stderr, "%d files and %d nodes: %d files per node" %\
        (len(files), len(nodes), fno)

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
        print >> sys.stderr, "Couldn't mkdir %s:%s/data/%s" % (node, root, name)

    print >> sys.stderr, "Copying %d files to %s" % (len(nset), node)
    p = Popen("nice -19 %s scp %s %s %s:%s/data/%s/" % 
        (ionice, " ".join(ssh), " ".join(nset), unode, root, name),
            shell = True)
    procs.append((node, p, nset))

print >> sys.stderr, "Waiting for copyers to finish.."

for node, p, nset in procs:
    if p.wait():
        print >> sys.stderr, "Copying to %s failed" % node
    else:
        print >> sys.stderr, "%s ok" % node
        print "\n".join(("disco://%s/%s/%s" %
            (hostname[node], name, os.path.basename(f))
                for f in nset))

print >> sys.stderr, "Done"


