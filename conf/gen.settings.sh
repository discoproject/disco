#!/bin/sh

cat << EOF
# --
# -- Disco settings
# --
# The defaults should be pretty sane, so be careful changing them.

# Home of the Disco libraries
DISCO_HOME = "${ABSTARGETLIB}"

# Root directory for Disco data
DISCO_ROOT = "${ABSTARGETSRV}"

# Where the master's web docroot lives
DISCO_WWW_ROOT = "${ABSTARGETDAT}/${WWW}"

# HTTP server for master and nodes runs on this port
# disco://host URIs are mapped to http://host:DISCO_PORT
DISCO_PORT = 8989

# Example config for Varnish proxy
# DISCO_PROXY_ENABLED = "on"
# DISCO_HTTPD = "/usr/sbin/varnishd -a 0.0.0.0:\$DISCO_PROXY_PORT -f \$DISCO_PROXY_CONFIG -P \$DISCO_PROXY_PID -n/tmp -smalloc"

DDFS_TAG_MIN_REPLICAS = 3
DDFS_TAG_REPLICAS     = 3
DDFS_BLOB_REPLICAS    = 3

EOF
