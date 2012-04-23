#!/bin/bash

cat << EOF
#!/bin/sh
### BEGIN INIT INFO
# Provides:          disco-master
# Required-Start:    \$network \$remote_fs
# Required-Stop:     \$network \$remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Disco master.
# Description:       The Disco master.
### END INIT INFO

. /lib/lsb/init-functions

[ -x ${RELBIN}/disco ] || exit 5

running() {
    [ -z "\$(${RELBIN}/disco status | grep stopped)" ]
    errcode=\$?
    return \$errcode
}

start_server() {
        ${RELBIN}/disco start
        errcode=\$?
	return \$errcode
}

stop_server() {
        ${RELBIN}/disco stop
        errcode=\$?
	return \$errcode
}

restart_server() {
        ${RELBIN}/disco restart
        errcode=\$?
        return \$errcode
}

case "\$1" in
  start)
        if running ;  then
            log_success_msg "disco-master already running"
            exit 0
        fi
        if start_server ; then
            log_success_msg "disco-master started"
            exit 0
        else
            log_failure_msg "disco-master couldn't be started"
            exit 1
        fi
	;;
  stop)
        if running ; then
            if stop_server ; then
                log_success_msg "disco-master stopped"
                exit 0
            else
                log_failure_msg "disco-master couldn't be stopped"
                exit 1
            fi
        else
            log_daemon_msg "disco-master already stopped"
            exit 0
        fi
        ;;
  restart|force-reload)
        if restart_server ; then
            log_success_msg "disco-master restarted"
            exit 0
        else
            log_failure_msg "disco-master couldn't be restarted"
            exit 1
        fi
	;;
  status)
        if running ;  then
            log_success_msg "disco-master running"
            exit 0
        else
            log_success_msg "disco-master stopped"
            exit 3
        fi
        ;;
  *)
	echo "Usage: \$0 {start|stop|restart|force-reload|status}" >&2
	exit 3
	;;
esac
EOF