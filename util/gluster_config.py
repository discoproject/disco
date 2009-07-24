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

DEFAULT_REPL_PORT = 9900
DEFAULT_NUFA_PORT = 9800

def check_config(config, replicas = True):
        REQ = ["nodes", "volumes", "master", "config_dir"]
        if replicas:
                REQ.append("replicas")
                if config["replicas"] > len(config["nodes"]):
                        print "replicas must be less than equal"\
                              "to the number of nodes."
                        print "Check the config file."
                        sys.exit(1)
        for k in REQ:
                if k not in config:
                        print "Required field '%s' is missing." % k
                        print "Check the config file."
                        sys.exit(1)

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
               "remote-port": config["port"],
               "ping-timeout": "60"}
        
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

def client_sect_nufa(f, config):
        print >> f, "\n# ------\n# NUFA CLIENT\n# ------\n"
        nv = len(config["volumes"])
        s = ["%s-vol%d" % (n, v + 1) for n in config["nodes"]
                        for v in range(nv)]
        output_volume(f, "nufa", "cluster/nufa", s,\
                {"local-volume-name": "`echo \"$(hostname)-vol1\"`"})

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
                        output_volume(f, "%s-lock" % vol,
                                "features/locks",
                                ["%s-posix" % vol])
                        output_volume(f, "%s-readahead" % vol,
                                "performance/read-ahead",
                                ["%s-lock" % vol], {
                                "page-size": "512KB", 
                                "page-count": "4"})
                        output_volume(f, vol, "performance/io-threads",
                                ["%s-readahead" % vol], {"thread-count": "32"})
        else:
                print >> f, "# this is just a dummy volume as gluster"
                print >> f, "# requires us to specify a subvolume for"
                print >> f, "# protocol/server. Nothing is actually saved"
                print >> f, "# here."
                path = os.path.abspath(config["config_dir"])
                output_volume(f, "masterdummy", "storage/posix",
                        [], {"directory": "/tmp"})
                subvol = ["masterdummy"]

        print >> f, "# -- server\n" 
        options.update(("auth.addr.%s.allow" % v, "*") for v in subvol)
        options.update({
                "transport-type": "tcp",
                "transport.socket.listen-port": config["port"]})
        output_volume(f, "server", "protocol/server", subvol, options)

def create_replicating_config(config, path):
        config["port"] = config.get("port", DEFAULT_REPL_PORT)
        client_conf = cStringIO.StringIO()
        nodes_sect(client_conf, config)
        repl_sect(client_conf, config)
        client_sect(client_conf, config)

        nodecfg = os.path.join(path, "inputfs_node.vol")
        print "Writing node config to %s.." % nodecfg
        nodef = file(nodecfg, "w")
        server_sect(nodef, config, {}, True)
        nodef.write(client_conf.getvalue())
        nodef.close()
        print "ok"
        return client_conf

def create_nufa_config(config, path):
        if len(config["volumes"]) > 1:
            print "Specify only one volume for results"
            sys.exit(1)
        if config["master"] not in config["nodes"]:
            print "Add your master node '%s' to the list of nodes" %\
                config["master"]
            sys.exit(1)
        
        config["port"] = config.get("port", DEFAULT_NUFA_PORT)

        client_conf = cStringIO.StringIO()
        nodes_sect(client_conf, config)
        client_sect_nufa(client_conf, config)
        
        nodecfg = os.path.join(path, "resultfs_node.vol")
        print "Writing node config to %s.." % nodecfg
        nodef = file(nodecfg, "w")
        server_sect(nodef, config, {}, True)
        nodef.write(client_conf.getvalue())
        nodef.close()
        print "ok"
        return client_conf

def create_master_config(name, config, path, client_conf):
        mastercfg = os.path.join(path, "%s_master.vol" % name)
        nodecfg = os.path.join(path, "%s_node.vol" % name)
        print "Writing master config to %s.." % mastercfg
        masterf = file(mastercfg, "w")
        server_sect(masterf, config,
                {"volume-filename.glu_node": nodecfg},
                config["master"] in config["nodes"])
        masterf.write(client_conf.getvalue())
        masterf.close()
        print "ok"

if len(sys.argv) < 3 or sys.argv[1] not in ["inputfs", "resultfs"]:
        print "\nUsage: python gluster_config.py [inputfs|resultfs] config.json"
        print 
        print "This script generates Disco-compatible config files for Gluster,"
        print "a distributed filesystem."
        print 
        print "Two modes are available:"
        print "- inputfs, which produces a Gluster volfile that is suitable for"
        print "  storing input data for Disco so that data is k-way replicated"
        print "  over nodes."
        print "- resultfs, which produces a Gluster volfile for communication"
        print "  between Disco nodes, in place of the default HTTP-based"
        print "  solution."
        print 
        print "See gluster_example.json for an example config gile. For more"
        print "information, see http://discoproject.org/doc/start/dfs.html.\n"
        sys.exit(1)

config = cjson.decode(file(sys.argv[2]).read())
path = os.path.abspath(config["config_dir"])

if sys.argv[1] == "inputfs":
        check_config(config, replicas = True)
        client = create_replicating_config(config, path)
elif sys.argv[1] == "resultfs":
        check_config(config, replicas = False)
        client = create_nufa_config(config, path)
create_master_config(sys.argv[1], config, path, client)

