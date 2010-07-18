
-record(dnode, {blacklisted=false :: bool() | 'manual' | timer:timestamp(),
                name :: nonempty_string(),
                node_mon :: pid(),
                num_running=0 :: non_neg_integer(),
                slots :: non_neg_integer(),
                stats_ok=0 :: non_neg_integer(),
                stats_failed=0 :: non_neg_integer(),
                stats_crashed=0 :: non_neg_integer()}).
-type dnode() :: #dnode{}.

-record(jobinfo, {force_local :: bool(),
                  force_remote :: bool(),
                  inputs :: [binary()] | [[binary()]],
                  map :: bool(),
                  max_cores :: non_neg_integer(),
                  nr_reduce :: non_neg_integer(),
                  reduce :: bool()}).
-type jobinfo() :: #jobinfo{}.

-record(task, {chosen_input :: binary(),
               force_local :: bool(),
               force_remote :: bool(),
               from :: pid(),
               input :: [{binary(), nonempty_string()}],
               jobname :: nonempty_string(),
               mode :: nonempty_string(), %"map" | "reduce"
               taskid :: non_neg_integer(),
               taskblack :: list()}).
-type task() :: #task{}.
