#!/usr/bin/python
import sys, os, re, cStringIO, time
from urllib import urlopen
from subprocess import *

SOURCES_LIST = """
deb %s /
deb http://http.us.debian.org/debian   lenny     main contrib non-free
deb http://security.debian.org     lenny/updates main contrib non-free
""" % (os.environ.get("DISCO_SOURCE", "http://discoproject.org/debian"))

def msg(s):
    print >> sys.stderr, s


def die(s):
    msg(s)
    sys.exit(1)


def check_path(path):
    if not os.path.exists(path):
        die("No such file: " + path)
    return path


def ssh(url, cmd, **args):
    args["stdout"] = args.get("stdout", VERBOSE_OUT)
    args["stderr"] = args.get("stderr", STDOUT)
    return Popen(["ssh", "-q", "-o", "UserKnownHostsFile=/dev/null",
        "-o", "StrictHostKeyChecking=no", "-i", KEYPATH,
        "root@" + url, cmd], **args)


def open_ssh_pipe(url):
    print >> sys.stderr, "Opening ssh tunnel to the master..",
    p = Popen(["ssh", "-o", "UserKnownHostsFile=/dev/null",
        "-o", "StrictHostKeyChecking=no", "-i", KEYPATH,
        "-L", "8989:localhost:8989", "-N", "-n",
        "root@" + url], stdout = VERBOSE_OUT, stderr = STDOUT)
    time.sleep(1)
    try:
        urlopen("http://localhost:8989/disco/ctrl/joblist").read()
    except:
        die("SSH tunnel failed or Disco master is not alive.")
    print >> sys.stderr, "ok. Tunnel PID is %d." % p.pid


def find_instances():
    p = Popen(["%s/ec2-describe-instances" % EC2BIN], stdout = PIPE)
    return [i for i in re.findall("INSTANCE\t(.+?)\t.*?\t(.+?)\t.*"\
        "running\t%s.*" % KEYPAIR, p.stdout.read())]


def process_instances(instances, op, m):
    proc = [(inst, op(inst, url)) for inst, url in instances]
    print >> sys.stderr, "%s:" % m,
    for i, p in enumerate(proc):
        if p[1].wait():
            die("Operation failed on instance %s." % inst)
        print >> sys.stderr, i + 1,
    print >> sys.stderr, "ok."


def distribute_file(instances, path, content, change_owner = False):
    def copy_file(inst, url):
        own = ""
        dir = os.path.dirname(path) 
        if change_owner:
			own = "touch %s; chmod 400 %s;"\
                  "chown -R disco:disco %s;" % (path, path, dir)
        p = ssh(url, "mkdir %s 2>/dev/null;%s cat > %s"\
            % (dir, own, path), stdin = PIPE)
        p.stdin.write(content)	
        p.stdin.close()
        return p
    process_instances(instances, copy_file, "Copy %s" % path)


def install_packages(inst, url):
    if inst == MASTER[0]:
        pack = "disco-master"
    else:
        pack = "disco-node"
    return ssh(url, "apt-get update; DEBIAN_FRONTEND=noninteractive "\
            "apt-get --force-yes -y install " + pack)

def make_sshkey(master):
       
       print >> sys.stderr, "Generating an ssh key on the master..",
       
       if ssh(master, "echo 'StrictHostKeyChecking no' >> "\
        "/etc/ssh/ssh_config").wait():
        die("Couldn't modify ssh_config")
       
       p = ssh(master, "rm -Rf /srv/disco/.ssh/; mkdir /srv/disco/.ssh && "\
        "ssh-keygen -N '' -f /srv/disco/.ssh/id_dsa >/dev/null && "\
        "chown -R disco:disco /srv/disco/.ssh/ && "\
        "cat /srv/disco/.ssh/id_dsa.pub", stdout = PIPE)
       key = p.stdout.read()
       if p.wait():
           die("Key generation failed.")
       print >> sys.stderr, "ok."
       return key

def get_cookie(master):
    p = ssh(master, "cat /srv/disco/.erlang.cookie", stdout = PIPE)
    cookie = p.stdout.read()
    if p.wait():
        die("Couldn't read ~/.erlang.cookie")
    return cookie

def make_hosts(master, instances):
    p = ssh(master, "bash -s", stdin = PIPE, stdout = PIPE, bufsize = 0)
    hosts = cStringIO.StringIO()
    fmt = "node%%.%dd\n" % len(str(len(instances) - 1))
    n = 1
    
    f = file("ec2-nodes", "w")

    for inst, url in instances:
        p.stdin.write("ping -c 1 %s | head -1\n" % url)
        m = re.search(" \((.*?)\) ", p.stdout.readline())
        if url == master:
            hosts.write("%s\tmaster\n" % m.group(1))
        else:
            hosts.write("%s\t" % m.group(1))
            hosts.write(fmt % n)
            print >> f, "%s\t%s" % (url, fmt % n),
            n += 1

    cluster_spec = "%s:%d" % ((fmt % 1).strip(), n - 1)
    return hosts.getvalue(), cluster_spec



def update_discoconfig(master, cluster_spec):
    print >> sys.stderr, "Updating disco config..",
    p = ssh(master, "cat /proc/cpuinfo | grep processor", stdout = PIPE)
    num_cores = len(p.stdout.readlines())
    try:
        r = urlopen("http://localhost:8989/disco/ctrl/save_config_table",
            '[["%s","%d"]]' % (cluster_spec, num_cores))
        if not r.headers.getheader("status").startswith("200"):
            die("Couldn't update disco config. "
                "Cluster spec is: %s, %d" % (cluster_spec, num_cores))
    except:
        die("Couldn't connect to Disco master")
    print >> sys.stderr, "ok."


if __name__ == "__main__":
    if len(sys.argv) < 2:
        die("Usage: setup-instances.py <aws-keypair-path> "\
            "[master-instance-id]")

    if "EC2_HOME" not in os.environ:
        die("EC2_HOME not set")

    if "VERBOSE" in os.environ:
        VERBOSE_OUT = None
    else:
        VERBOSE_OUT = file("/dev/null", "w")

    EC2BIN = os.environ["EC2_HOME"] + "/bin"

    KEYPATH = check_path(sys.argv[1])
    KEYPAIR = os.path.basename(sys.argv[1])

    msg("Finding instances..")
    instances = find_instances()

    if len(instances):
        msg("Found %d instances:\n%s" % (len(instances),
            "\n".join(map(lambda x: "%d. %s at %s" %
                (x[0] + 1, x[1][0], x[1][1]),
                    enumerate(instances)))))
    else:
        die("No EC2 instances found. Start some with ec2-run-instances "
            "or wait for a while if they are pending.")
    
    if len(sys.argv) > 2:
        MASTER = sys.argv[1]
        m = [(i, url) for i, url in instances if i == MASTER]
        if not m:
            die("Could not find the master instance %s." % MASTER)
        MASTER = m[0]
    else:
        MASTER = instances[0]

    msg("Master is %s." % MASTER[0])
    
    distribute_file(instances, "/etc/apt/sources.list", SOURCES_LIST)
    
    hosts, cluster_spec = make_hosts(MASTER[1], instances)
    hosts += "\n127.0.0.1\tlocalhost\n"
    distribute_file(instances, "/etc/hosts", hosts)

    process_instances(instances, install_packages, "Install packages")
    
    key = make_sshkey(MASTER[1])
    distribute_file(instances, "/srv/disco/.ssh/authorized_keys", key,
        change_owner = True)
       
    cookie = get_cookie(MASTER[1])
    distribute_file(instances, "/srv/disco/.erlang.cookie", cookie,
        change_owner = True)

    open_ssh_pipe(MASTER[1])
    
    update_discoconfig(MASTER[1], cluster_spec)
    
    print >> sys.stderr, "Nodes written to ec2-nodes. "\
        "Use this file with distrfiles.py."

    print >> sys.stderr, "All done!\n\n"\
    "Disco master is now ready for use at http://localhost:8989.\n"\
    "Remember to set DISCO_PROXY=http://localhost:8989 for Disco clients "\
    "as results must be accessed through the master.\n"
    


    

