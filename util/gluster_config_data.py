"""
Example config (must be valid JSON):

-- snip --

{
        "nodes": ["node01", "node02", "node03"],
        "volumes": ["/mnt/disk1", "/mnt/disk2"],
        "replicas": 2,
        "master": "nxfront",
        "config_dir": "/tmp/config"
}

-- snip --

where

- nodes: nodes in the disco cluster
- volumes: where disks are mounted on nodes
- nreplicas: desired number of replicas, must be =< len(nodes)
- master: master hostname
- config_dir: where to store config files on the master node

"""
import sys, cjson, md5, cStringIO, os

DEFAULT_PORT = 9999

def check_config(config):
        REQ = ["nodes", "volumes", "replicas", "master", "config_dir"]
        for k in REQ:
                if k not in config:
                        print "Required field '%s' is missing." % k
                        print "Check the config file."
                        sys.exit(1)
        if config["replicas"] > len(config["nodes"]):
                print "replicas must be less than equal to the number of nodes."
                print "Check the config file."
                sys.exit(1)
        config["port"] = config.get("port", DEFAULT_PORT)

def output_volume(f, name, type, subvol = None, options = {}):
        print >> f, "volume %s" % name
        print >> f, "    type %s" % type
        opt = sorted(options.keys())
        for k in sorted(options.keys()):
                print >> f, "    option %s %s" % (k, options[k])
        if subvol:
                print >> f, "    subvolumes %s" % " ".join(subvol)
        print >> f, "end-volume\n"

def nodes_sect(f, config):
        print >> f, "\n# -----\n# NODES\n# -----\n"
        opt = {"transport-type": "tcp",
               "remote-port": config["port"]}
        
        for node in config["nodes"]:
                print >> f, "# -- %s\n" % node
                opt["remote-host"] = node
                for v in range(len(config["volumes"])):
                        opt["remote-subvolume"] = vol = "vol%d" % (v + 1)
                        output_volume(f, "%s-%s" % (node, vol),
                                "protocol/client", [], opt)

def repl_sect(f, config):
        print >> f, "\n# -----------\n# REPLICATION\n# -----------\n"
        hashes = sorted((int(md5.md5(node).hexdigest(), 16), node)\
                for node in config["nodes"])
        k = config["replicas"]
        for i, h in enumerate(hashes):
                print >> f, "# -- %s (%s)\n" % (h[1], hex(h[0])[2:-1])
                repl = [hashes[i - j][1] for j in range(k)]
                for v in range(len(config["volumes"])):
                        vol = "vol%d" % (v + 1)
                        name = "%s-%s-repl" % (h[1], vol)
                        r = ["%s-%s" % (x, vol) for x in repl]
                        output_volume(f, name, "cluster/replicate", r,
                                {"read-subvolume": "`echo \"$(hostname)-%s\"`" 
                                        % vol})

def client_sect(f, config):
        print >> f, "\n# ------\n# CLIENT\n# ------\n"
        nv = len(config["volumes"])
        s = ["%s-vol%d-repl" % (n, v + 1) for n in config["nodes"]
                        for v in range(nv)]
        output_volume(f, "distribute", "cluster/distribute", s)

def server_sect(f, config, options, writevol):
        print >> f, "\n# ------\n# SERVER\n# ------\n"
        subvol = []
        if writevol:
                for v, dir in enumerate(config["volumes"]):
                        vol = "vol%d" % (v + 1)
                        print >> f, "# -- %s\n" % vol
                        output_volume(f, "%s-posix" % vol, "storage/posix",
                                [], {"directory": dir})
                        subvol.append(vol)
                        output_volume(f, "%s-lock" % vol, "features/locks",
                                ["%s-posix" % vol])
                        output_volume(f, vol, "performance/read-ahead",
                                ["%s-lock" % vol], {
                                        "page-size": "512KB", 
                                        "page-count": "4"})
        else:
                print >> f, "# this is just a dummy volume as gluster"
                print >> f, "# requires us to specify a subvolume for"
                print >> f, "# protocol/server. Nothing is actually saved"
                print >> f, "# here."
                path = os.path.abspath(config["config_dir"])
                output_volume(f, "masterdummy", "storage/posix",
                        [], {"directory": path})
                subvol = ["masterdummy"]

        print >> f, "# -- server\n" 
        options.update(("auth.addr.%s.allow" % v, "*") for v in subvol)
        options.update({
                "transport-type": "tcp",
                "transport.socket.listen-port": config["port"]})
        output_volume(f, "server", "protocol/server", subvol, options)


if len(sys.argv) < 2:
        print "\nGiven a file, config.json, that specifies available nodes,"
        print "generates config files for Gluster, a distributed filesystem."
        print "The resulting filesystem is suitable for storing input data for"
        print "Disco so that data is k-way replicated over the nodes. See"
        print "the source of this file for an example config file.\n"
        print "If you want to use Gluster also for internal communication of "
        print "Disco (instead of HTTP), run gluster_config_comm.py.\n"
        print "Usage: python gluster_config_data.py config.json\n"
        sys.exit(1)

config = cjson.decode(file(sys.argv[1]).read())
check_config(config)

client_conf = cStringIO.StringIO()
nodes_sect(client_conf, config)
repl_sect(client_conf, config)
client_sect(client_conf, config)

path = os.path.abspath(config["config_dir"])
nodecfg = os.path.join(path, "glu_node.vol")
print "Writing node config to %s.." % nodecfg
nodef = file(nodecfg, "w")
server_sect(nodef, config, {}, True)
nodef.write(client_conf.getvalue())
nodef.close()
print "ok"

mastercfg = os.path.join(path, "glu_master.vol")
print "Writing master config to %s.." % mastercfg
masterf = file(mastercfg, "w")
server_sect(masterf, config,
        {"volume-filename.glu_node": nodecfg},
        config["master"] in config["nodes"])
masterf.write(client_conf.getvalue())
masterf.close()
print "ok"


