-module(fair_scheduler).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

-include("task.hrl").

start_link() ->
        error_logger:info_report([{"Fair scheduler starts"}]),
        case gen_server:start_link({local, scheduler}, fair_scheduler, [],
                        disco_server:debug_flags("fair_scheduler")) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init([]) ->
        case application:get_env(scheduler_opt) of
                {ok, "fifo"} ->
                        error_logger:info_report(
                                [{"Scheduler uses fifo policy"}]),
                        fair_scheduler_fifo_policy:start_link();
                _ ->
                        error_logger:info_report(
                                [{"Scheduler uses fair policy"}]),
                        fair_scheduler_fair_policy:start_link()
        end,
        ets:new(jobs, [private, named_table]),
        {ok, []}.

handle_cast({update_nodes, NewNodes}, _) ->
        gen_server:cast(sched_policy, {update_nodes, NewNodes}),
        Msg = {update_nodes, NewNodes},
        [gen_server:cast(JobPid, Msg) || {_, JobPid} <- ets:tab2list(jobs)],
        {noreply, NewNodes};

handle_cast({job_done, JobName}, Nodes) ->
        error_logger:info_report({"Scheduler removes", JobName}),
        ets:delete(jobs, JobName),
        {noreply, Nodes}.

% This is not a handle_cast function, since we don't want to race against
% disco_server. We need to send the new_job and new_task messaged before
% disco_server sends its task_started and next_task messages. 
handle_call({new_task, Task}, _, Nodes) ->
        JobName = Task#task.jobname,
        Job = case ets:lookup(jobs, JobName) of
                [] ->
                        error_logger:info_report({"NEW JOB", JobName}),
                        {ok, JobPid} = fair_scheduler_job:start(
                                Task#task.jobname, Task#task.from),
                        gen_server:cast(JobPid, {update_nodes, Nodes}),
                        gen_server:cast(sched_policy,
                                {new_job, JobPid, JobName}),
                        ets:insert(jobs, {JobName, JobPid}),
                        JobPid;
                [{_, JobPid}] -> JobPid
        end,
        gen_server:cast(Job, {new_task, Task}),
        {reply, ok, Nodes};

handle_call({next_task, AvailableNodes}, _From, Nodes) ->
        Jobs = [JobPid || {_, JobPid} <- ets:tab2list(jobs)],
        {reply, next_task(AvailableNodes, Jobs, []), Nodes}.

next_task(AvailableNodes, Jobs, NotJobs) ->
        case gen_server:call(sched_policy, {next_job, NotJobs}) of
                {ok, JobPid} -> 
                        case fair_scheduler_job:next_task(
                                        JobPid, Jobs, AvailableNodes) of
                                {ok, Task} ->
                                        {ok, {JobPid, Task}};
                                none ->
                                        next_task(AvailableNodes,
                                                Jobs, [JobPid|NotJobs])
                        end;
                nojobs -> nojobs
        end.

handle_info(_Msg, State) -> {noreply, State}. 

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
