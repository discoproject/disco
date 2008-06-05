if [ ! -e disco.cfg ]
then
        echo "[]" >> disco.cfg
fi

if [ -z $DISCO_PORT ]
then
        DISCO_PORT=4444
fi

if [ -z $PREFIX ]
then
	PREFIX="disco"
fi

export SLAVENAME=$PREFIX"_slave"

PATH=.:$PATH erl +K true -sname $PREFIX"_master" -rsh ssh -smp on -pa ebin -pa src -boot disco -disco scgi_port $DISCO_PORT -disco disco_config disco.cfg -kernel error_logger '{file, "disco.log"}' -eval "[handle_job, handle_ctrl]"
