% Utility types.

-type next_job() :: {ok, pid()} | nojobs.
-type loadstats() :: disco_gbtree(host(), non_neg_integer()). % #running tasks
-type priority() :: float().
-type node_info() :: {host(), cores(), non_neg_integer()}.

% Common scheduler msgs.

-type new_task_msg()     :: {new_task, task(), loadstats()}.
-type update_nodes_msg() :: {update_nodes, [node_info()]}.

-type new_job_msg()  :: {new_job, pid(), jobname()}.
-type next_job_msg() :: {next_job, [pid()]}.

-type current_priorities_msg() :: current_priorities.

-type policy_cast_msgs() :: new_job_msg() | next_job_msg() | update_nodes_msg()
                          | current_priorities_msg().
