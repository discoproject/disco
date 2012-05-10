-type priority() :: float().

% The messages received by the scheduler policy.

-type new_job_msg()  :: {new_job, pid(), jobname()}.
-type next_job_msg() :: {next_job, [pid()]}.

-type update_nodes_msg()       :: {update_nodes, [{host_name(), cores()}]}.
-type current_priorities_msg() :: current_priorities.

-type public_cast_msgs() :: new_job_msg() | next_job_msg() | update_nodes_msg()
                          | current_priorities_msg().

% Utility types.
-type next_job() :: {ok, pid()} | nojobs.
