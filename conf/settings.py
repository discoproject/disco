# --
# -- Variables below this line rarely need to be modified
# --

# HTTP server for master and nodes runs on this port.
# disco://host URIs are mapped to http://host:DISCO_PORT.
DISCO_PORT = 8989

# Example config for Varnish proxy
# DISCO_PROXY_ENABLED = "on"
# DISCO_HTTPD = "/usr/sbin/varnishd -a 0.0.0.0:\$DISCO_PROXY_PORT -f \$DISCO_PROXY_CONFIG -P \$DISCO_PROXY_PID -n/tmp -smalloc"

DDFS_TAG_MIN_REPLICAS = 3
DDFS_TAG_REPLICAS     = 3
DDFS_BLOB_REPLICAS    = 3
