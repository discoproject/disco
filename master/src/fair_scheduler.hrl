% Utility types.

-type next_job() :: {ok, pid()} | nojobs.
-type nodestat() :: {false | non_neg_integer(), task_input()}.
-type priority() :: float().

% Common scheduler msgs.

-type new_task_msg()     :: {new_task, task(), [nodestat()]}.
-type update_nodes_msg() :: {update_nodes, [{host_name(), cores()}]}.

-type new_job_msg()  :: {new_job, pid(), jobname()}.
-type next_job_msg() :: {next_job, [pid()]}.

-type current_priorities_msg() :: current_priorities.

-type policy_cast_msgs() :: new_job_msg() | next_job_msg() | update_nodes_msg()
                          | current_priorities_msg().
