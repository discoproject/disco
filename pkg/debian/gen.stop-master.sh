#!/bin/bash

cat << EOF
#!/bin/sh

set -e

invoke-rc.d --quiet disco-master stop || true

exit 0
EOF
