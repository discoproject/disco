-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-type event() :: none
               | {job_data, jobinfo()}
               | {task_start, stage_name()}
               | {task_pending, stage_name()}
               | {task_un_pending, stage_name()}
               | {task_ready, stage_name()}
               | {task_failed, stage_name()}
               | {stage_start, stage_name(), integer()}
               | {stage_ready, stage_name(), [[url()]]}
               | {ready, [url() | [url()]]}.

-type job_status() :: active | dead | ready.


-type job_eventinfo() :: {StartTime :: erlang:timestamp(),
                          Status  :: job_status(),
                          JobInfo :: none | jobinfo(),
                          Results :: [[url()]],
                          Count   :: stage_dict(),
                          Pending :: stage_dict(),
                          Ready   :: stage_dict(),
                          Failed  :: stage_dict()}.

