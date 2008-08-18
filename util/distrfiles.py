import os, sys, math, random, os
from subprocess import *
 
files = os.listdir(sys.argv[1])
if sys.argv[1][-1] == "/":
	name = os.path.basename(sys.argv[1][:-1])
else:
	name = os.path.basename(sys.argv[1])

nodes = file(sys.argv[2]).readlines()
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
	node = nodes.next().strip()
	
	if "REMOVE_FIRST" in os.environ:
		call(["ssh", node, "rm -Rf /var/disco/" + name])

	if call(["ssh", node, "mkdir /var/disco/%s" % name]):
		print >> sys.stderr, "Couldn't mkdir %s:/var/disco/%s" % (node, name)

	print >> sys.stderr, "Copying %d files to %s" % (len(nset), node)
	p = Popen("nice -19 ionice -n 7 scp " + " ".join(nset) +
                  " %s:/var/disco/%s/" % (node, name), shell = True)
	procs.append((node, p, nset))

print >> sys.stderr, "Waiting for copyers to finish.."

for node, p, nset in procs:
	if p.wait():
		print >> sys.stderr, "Copying to %s failed" % node
	else:
		print >> sys.stderr, "%s ok" % node
		print "\n".join(("disco://%s/%s/%s" %
                        (node, name, os.path.basename(f)) for f in nset))

print >> sys.stderr, "Done"


