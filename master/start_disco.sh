if [ ! -e disco.cfg ]
then
        echo "[]" >> disco.cfg
fi

PATH=.:$PATH erl +K true -smp on -pa ebin -pa src -boot disco -disco scgi_port 4444 -disco disco_config disco.cfg -kernel error_logger '{file, "disco.log"}' -eval "[handle_job, handle_ctrl]"
