-module(fair_scheduler).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

-define(ACCOUNTING_INTERVAL, 1000).

start_link() ->
        error_logger:info_report([{"Fair scheduler starts"}]),
        case gen_server:start_link({local, scheduler},
                        fair_scheduler, [], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init() ->
        case application:get_env(disco_scheduler_opt) of
                {ok, "fifo"} ->
                        error_logger:info_report(
                                [{"Scheduler uses fifo policy"}]),
                        spawn_link(fun() -> fifo_policy_proc() end);
                _ ->
                        error_logger:info_report(
                                [{"Scheduler uses fair policy"}]),
                        spawn_link(fun() -> fair_policy_proc() end)
        end,
        ets:new(jobs, [private, named_table]),
        {ok, Nodes}.

handle_cast({update_nodes, NewNodes}, Nodes) ->
        job_cast({update_nodes, Nodes}),
        {noreply, Nodes};

handle_cast({new_task, Task}, Nodes) ->
        JobPid = case ets:lookup(jobs, Task#task.jobname) of
                [] ->
                        {ok, Job} = fair_scheduler_job:start(
                                Task#task.jobname, Task#task.from),
                        gen_server:cast(Job, {update_nodes, Nodes}),
                        ets:insert(jobs, {Task#task.jobname, Job}),
                        policy ! {new_job, Job},
                        Job;
                [{_, Pid}] -> Pid
        end,
        gen_server:cast(JobPid, {new_task, Task}),
        {noreply, Nodes}.

handle_call(_Msg, _From, State) ->
        {reply, ok, State}.


job_cast(Msg) ->
        [gen_server:cast(Job, Msg) || {_, Job} <- ets:tab2list(jobs)].




%
% Fair Accounting
%

%accounting_proc() ->
%        true = register(accounting, self()),
%        accounting_loop(accounting_update_stats(gb_trees:empty())).
%
%accounting_loop(Stats) ->
%        receive
%                {new_job, Job} ->
%                        NStats = gb_trees:insert(Job, 0.0, Stats),
%                        erlang:monitor(process, Job),
%                        accounting_loop(NStats);
%                {'DOWN', _, _, Job, _} ->
%                        accounting_loop(gb_trees:delete(Job, Stats));
%                update_stats ->
%                        accounting_loop(accounting_update(Stats))
%        end.
%
%accounting_update_stats(Stats) ->
%         
%        timer:send_after(?ACCOUNTING_INTERVAL, update_stats).



% unused


handle_info(_Msg, State) -> {noreply, State}. 

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
