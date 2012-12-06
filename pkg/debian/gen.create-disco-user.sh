#!/bin/bash

cat << EOF
#!/bin/sh

set -e

case "\$1" in
    configure)
        if ! getent group | grep -q "^disco:" ; then
            addgroup --system \
                     --quiet \
                     disco 2>/dev/null
        fi
        if ! getent passwd | grep -q "^disco:" ; then
            adduser --system \
                    --quiet \
                    --group \
                    --disabled-password \
                    --shell /bin/bash \
                    --home ${RELSRV} \
                    --no-create-home \
                    disco 2>/dev/null
        fi
        usermod -c "Disco" -d ${RELSRV} -g disco disco
        chown disco:disco ${RELSRV}
    ;;

    abort-upgrade|abort-remove|abort-deconfigure)
    ;;

    *)
        echo "postinst called with unknown argument: \$1" >&2
        exit 1
    ;;
esac

# dh_installdeb will replace this with shell code automatically
# for details, see http://www.debian.org/doc/debian-policy/

#DEBHELPER#

exit 0
EOF