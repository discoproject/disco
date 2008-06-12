if [ ! -e disco.cfg ]
then
        echo "[]" >> disco.cfg
fi

if [ -z $DISCO_PORT ]
then
        DISCO_PORT=4444
fi

if [ -z $DISCO_NAME ]
then
	DISCO_NAME="disco"
fi

if [ -z $DISCO_ROOT ]
then
	DISCO_ROOT="/var/disco/"
fi

DISCO_ROOT="$DISCO_ROOT/_$DISCO_NAME"

mkdir -p $DISCO_ROOT

if [ ! -e $DISCO_ROOT ]
then
	echo "$DISCO_ROOT doesn't exist"
	exit 1
fi

if [ -z $DISCO_URL ]
then
	echo "DISCO_URL not specified"
	exit 1
fi

DISCO_URL="$DISCO_URL/_$DISCO_NAME/"

PATH=.:$PATH erl +K true
                 -sname $DISCO_NAME"_master"\
                 -rsh ssh\
                 -smp on\
                 -pa ebin -pa src\
                 -boot disco\
                 -disco disco_slave \"$DISCO_NAME"_slave"\"\
                 -disco disco_root \"$DISCO_ROOT\"\
                 -disco disco_url \"$DISCO_URL\" \
                 -disco scgi_port $DISCO_PORT\
                 -disco disco_config disco.cfg\
                 -kernel error_logger '{file, "disco.log"}'\
                 -eval "[handle_job, handle_ctrl]"

