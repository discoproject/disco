#!/bin/bash

cat << EOF
#!/bin/sh

set -e

case "\$1" in
    purge)
        # we *do* delete the disco user, since we created it
        # we also delete the home (data) directory
        # but we should pay attention as the discussion unfolds:
        # http://bugs.debian.org/cgi-bin/bugreport.cgi?bug=621833
	deluser --quiet --system disco --remove-home > /dev/null || true
    ;;

    remove|upgrade|failed-upgrade|abort-install|abort-upgrade|disappear)
    ;;

    *)
        echo "postrm called with unknown argument: \$1" >&2
        exit 1
    ;;
esac

# dh_installdeb will replace this with shell code automatically
# for details, see http://www.debian.org/doc/debian-policy/

#DEBHELPER#

exit 0
EOF