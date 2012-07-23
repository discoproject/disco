-type jobname() :: nonempty_string().

-type cores() :: non_neg_integer().

-type job_inputs() :: [url() | [url()]].
-record(jobinfo, {jobname :: jobname(),
                  jobfile :: nonempty_string(),
                  jobenvs :: [{nonempty_string(), string()}],
                  force_local :: boolean(),
                  force_remote :: boolean(),
                  owner :: binary(),
                  inputs :: job_inputs(),
                  worker :: binary(),
                  map :: boolean(),
                  max_cores :: cores(),
                  nr_reduce :: non_neg_integer(),
                  reduce :: boolean()}).
-type jobinfo() :: #jobinfo{}.

-record(nodeinfo, {name :: host(),
                   connected :: boolean(),
                   blacklisted :: boolean(),
                   slots :: cores(),
                   num_running :: non_neg_integer(),
                   stats_ok :: non_neg_integer(),
                   stats_failed :: non_neg_integer(),
                   stats_crashed :: non_neg_integer()}).
-type nodeinfo() :: #nodeinfo{}.

-type task_input() :: {binary(), host()}.
-type task_mode() :: map | reduce.
-record(task, {chosen_input :: binary() | [binary()],
               force_local :: boolean(),
               force_remote :: boolean(),
               from :: pid(),
               input :: [task_input()],
               jobenvs :: [{nonempty_string(), string()}],
               worker :: binary(),
               jobname :: jobname(),
               mode :: task_mode(),
               taskid :: non_neg_integer(),
               taskblack :: [host()],
               fail_count :: non_neg_integer()}).
-type task() :: #task{}.

% types used for local-cluster mode

%                          {NextPort, {host() -> {GetPort, PutPort}}}.
-type port_map() :: none | {non_neg_integer(), gb_tree()}.

-record(node_ports, {get_port :: non_neg_integer(),
                     put_port :: non_neg_integer()}).
-type node_ports() :: #node_ports{}.
