
-record(task, {jobname, taskid, mode, taskblack, input, from,
               chosen_input, force_local, force_remote}).

-record(jobinfo, {nr_reduce, map, reduce, inputs,
                max_cores, force_local, force_remote}).
