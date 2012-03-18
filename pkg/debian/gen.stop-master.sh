#!/bin/bash

cat << EOF
#!/bin/sh
invoke-rc.d --quiet disco-master stop || exit 0
EOF
