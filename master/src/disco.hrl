
-record(dnode, {blacklisted=false,
                name,
                node_mon,
                num_running=0,
                slots,
                stats_ok=0,
                stats_failed=0,
                stats_crashed=0}).

-record(jobinfo, {force_local,
                  force_remote,
                  inputs,
                  map,
                  max_cores,
                  nr_reduce,
                  reduce}).

-record(task, {chosen_input,
               force_local,
               force_remote,
               from,
               input,
               jobname,
               mode,
               taskid,
               taskblack}).
