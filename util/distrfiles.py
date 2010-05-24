import os
import sys
import math
import random

from subprocess import Popen, call, PIPE

USAGE_INFO = """
Usage: distrfiles.py <dataset-path> <nodes-file>\n
DISCO_ROOT must specify the disco home directory (e.g. /srv/disco/).
SSH_KEY can specify a value for the -i flag of ssh / scp.
SSH_USER can specify the user name.
REMOVE_FIRST if set, removes previous dataset of the same name before copying.
DISABLE_IONICE if set, disables ionice even if it is found.
"""

def log(s):
    sys.stderr.write(s + '\n')


def copy_files(name, files, nodes):
    """Copy files over SSH to the specified nodes.

    @name - dataset name
    """
    fno = int(math.ceil(max(1, float(len(files)) / len(nodes))))
    log('%d files and %d nodes: %d files per node' %
        (len(files), len(nodes), fno))

    root = os.environ['DISCO_ROOT']
    ssh = ['-i', os.environ['SSH_KEY']] if 'SSH_KEY' in os.environ else []
    ssh_user = os.environ.get('SSH_USER', '')

    # check to see if ionice is installed on this system
    ionice = ''
    if 'DISABLE_IONICE' not in os.environ:
        status = Popen('which ionice'.split(), stdout=PIPE).wait()
        ionice = 'ionice -n 7' if status == 0 else ''

    fgroups = (files[idx:idx+fno] for idx in range(0, len(files), fno))
    procs = []

    for fnames, node in zip(fgroups, nodes):
        unode = node
        if ssh_user:
            unode = '%s@%s' % (ssh_user, node)

        if 'REMOVE_FIRST' in os.environ:
            call(['ssh'] + ssh + [unode, 'rm -Rf %s/data/%s' % (root, name)])

        if call(['ssh'] + ssh + [unode, 'mkdir /%s/data/%s' % (root, name)]):
            log("Couldn't mkdir %s:%s/data/%s" % (node, root, name))

        log('Copying %d files to %s' % (len(fnames), node))
        p = Popen('nice -19 %s scp %s %s %s:%s/data/%s/' %
                  (ionice, ' '.join(ssh), ' '.join(fnames), unode, root, name),
                  shell=True)
        procs.append((p, node, fnames))

    return procs


def main():
    dataset_path, nodes_path = sys.argv[1:]
    files = [os.path.join(dataset_path, fname)
             for fname in os.listdir(dataset_path)]
    name = os.path.basename(dataset_path.rstrip('/'))

    nodes, hostname = [], {}
    for line in open(nodes_path):
        s = line.strip().split()
        node = s[0].strip()
        hostname[node] = s[1] if len(s) > 1 else node
        nodes.append(node)

    log('Dataset name: %s' % name)

    random.shuffle(nodes)
    procs = copy_files(name, files, nodes)

    log('Waiting for copiers to finish..')

    for process, node, fnames in procs:
        if process.wait():
            log('Copying to %s failed' % node)
        else:
            log('%s ok' % node)
            print '\n'.join(('disco://%s/%s/%s' %
                             (hostname[node], name, os.path.basename(f))
                             for f in fnames))
    log('Done')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        log(USAGE_INFO)
        sys.exit(1)

    if 'DISCO_ROOT' not in os.environ:
        log('Specify DISCO_ROOT (data lives at $DISCO_ROOT/data)')
        sys.exit(1)

    main()
