
-module(fair_scheduler_fifo_policy).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
    handle_info/2, terminate/2, code_change/3]).

start_link() ->
    lager:info("Fair scheduler: FIFO policy"),
    case gen_server:start_link({local, sched_policy},
            fair_scheduler_fifo_policy, [],
            disco:debug_flags("fair_scheduler_fifo_policy")) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

init(_) ->
    {ok, queue:new()}.

handle_cast({update_nodes, _}, Q) ->
    {noreply, Q};

handle_cast({new_job, JobPid, JobName}, Q) ->
    erlang:monitor(process, JobPid),
    {noreply, queue:in({JobPid, JobName}, Q)}.

handle_call({next_job, NotJobs}, _, Q) ->
    {reply, dropwhile(Q, NotJobs), Q};

handle_call(current_priorities, _, Q) ->
    {reply, {ok, case queue:to_list(Q) of
        [{_, N}|R] ->
            [{N, -1.0}|[{M, 1.0} || {_, M} <- R]];
        [] -> []
    end}, Q};

handle_call(dbg_get_state, _, Q) ->
    {reply, Q, Q}.

dropwhile(Q, NotJobs) ->
    case queue:out(Q) of
        {{value, {JobPid, _}}, NQ} ->
            V = lists:member(JobPid, NotJobs),
            if V ->
                dropwhile(NQ, NotJobs);
            true ->
                {ok, JobPid}
            end;
        {empty, _} -> nojobs
    end.

handle_info({'DOWN', _, _, JobPid, _}, Q) ->
    L = queue:to_list(Q),
    {value, {_, JobName} = E} = lists:keysearch(JobPid, 1, L),
    gen_server:cast(scheduler, {job_done, JobName}),
    {noreply, queue:from_list(L -- [E])}.

% unused

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
