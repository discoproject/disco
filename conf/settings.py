# Disco settings
import os

# DISCO_HOME will be guessed according to the path that disco is called from,
# however its value can be overridden here for non-standard installations.

# Enable the following line if you run Disco on a single machine,
# or you have several masters running on the same server.
DISCO_MASTER_PORT = 7000

# Lighttpd for master and nodes runs on this port. 
# disco://host URIs are mapped to http://host:DISCO_PORT.
DISCO_PORT = 8989

# Port for master <-> lighty communication.
DISCO_SCGI_PORT = 4444

# Root directory for Disco data.
DISCO_ROOT = os.path.join(DISCO_HOME, 'root')

# Root directory for Disco binaries.
# Binaries must be found under DISCO_MASTER_HOME/ebin.
DISCO_MASTER_HOME = os.path.join(DISCO_HOME, 'master')

# Root directory for Disco logs.
DISCO_LOG_DIR = DISCO_HOME
DISCO_PID_DIR = DISCO_HOME

# Miscellaneous flags:
# - nocurl: use httplib instead of pycurl even if pycurl is available
#DISCO_FLAGS = "nocurl"

# The maximum amount of virtual memory available for master
# (ulimit -v DISCO_ULIMIT)
DISCO_ULIMIT = 16000000

# Specify hostname of your master node. Usually this is
# resolved correctly by the web server and DISCO_MASTER_HOST
# can be unspecified. If this fails, you can override the automatic
# guess and specify the hostname with DISCO_MASTER_HOST. This
# is often the case with Mac OS X.
DISCO_MASTER_HOST = "localhost"

DISCO_USER = "jflatow"

# get rid of this setting
# (should be able to mix slave types, and each node should be able to use uname if necessary)
DISCO_SLAVES_OS = "none"

