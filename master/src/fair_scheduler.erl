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
    NNodes = [Name || {Name, _NumCores} <- NewNodes],
    Msg = {update_nodes, NNodes},
    [gen_server:cast(JobPid, Msg) ||
        {_, {JobPid, _}} <- ets:tab2list(jobs)],
    {noreply, NNodes};

handle_cast({job_done, JobName}, Nodes) ->
    % We absolutely don't want to have the job coordinator alive after the
    % job has been removed from the scheduler. Make sure that doesn't
    % happen.
    case ets:lookup(jobs, JobName) of
        [] -> {noreply, Nodes};
        [{_, {_, JobCoord}}] ->
            ets:delete(jobs, JobName),
            exit(JobCoord, kill_worker),
            {noreply, Nodes}
    end.

handle_call({new_job, JobName, JobCoord}, _, Nodes) ->
    {ok, JobPid} = fair_scheduler_job:start(JobName, JobCoord),
    gen_server:cast(JobPid, {update_nodes, Nodes}),
    gen_server:cast(sched_policy, {new_job, JobPid, JobName}),
    ets:insert(jobs, {JobName, {JobPid, JobCoord}}),
    {reply, ok, Nodes};

% This is not a handle_cast function, since we don't want to race against
% disco_server. We need to send the new_job and new_task messaged before
% disco_server sends its task_started and next_task messages. 
handle_call({new_task, Task, NodeStats}, _, Nodes) ->
    JobName = Task#task.jobname,
    case ets:lookup(jobs, JobName) of
        [] ->
            {reply, unknown_job, Nodes};
        [{_, {JobPid, _}}] ->
            gen_server:cast(JobPid, {new_task, Task, NodeStats}),
            {reply, ok, Nodes}
    end;

handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call({next_task, AvailableNodes}, _From, Nodes) ->
    Jobs = [JobPid || {_, {JobPid, _}} <- ets:tab2list(jobs)],
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

terminate(_Reason, _State) ->
    [exit(JobPid, kill) || {_, {JobPid, _}} <- ets:tab2list(jobs)].

code_change(_OldVsn, State, _Extra) -> {ok, State}.
