
-record(task, {jobname :: nonempty_string(),
               taskid :: non_neg_integer(),
               mode :: nonempty_string(), %"map" | "reduce"
               taskblack :: list(),
               input :: [{binary(), nonempty_string()}],
               from :: pid(),
               chosen_input :: binary(),
               force_local :: boolean() ,
               force_remote :: boolean()}).
-type task() :: #task{}.

-record(jobinfo, {nr_reduce :: non_neg_integer(),
                  map :: boolean(),
                  reduce :: boolean(),
                  inputs :: [binary()] | [[binary()]],
                  max_cores :: non_neg_integer(),
                  force_local :: boolean(),
                  force_remote :: boolean()}).
-type jobinfo() :: #jobinfo{}.
