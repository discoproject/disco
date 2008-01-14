
erl +K true -smp on -pa ebin -pa src -boot disco -disco scgi_port 2222 -disco disco_config disco.cfg -eval "[handle_job, handle_ctrl]"
