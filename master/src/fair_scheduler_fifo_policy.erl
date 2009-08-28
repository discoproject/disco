
-module(fair_scheduler_fifo_policy).
-behaviour(gen_server).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

start_link(Nodes) ->
        error_logger:info_report([{"Fair scheduler: FIFO policy"}]),
        case gen_server:start_link({local, sched_policy}, 
                        fair_scheduler_fifo_policy, Nodes, []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

init(_) ->
        {ok, queue:new()}.

handle_cast({update_nodes, _}, Q) ->
        {noreply, Q};

handle_cast({new_job, JobPid, JobName}, Q) ->
        erlang:monitor(process, JobPid),
        {noreply, queue:in(JobPid, Q)}.

handle_call({next_job, NotJobs}, _, Q) ->
        {reply, dropwhile(Q, NotJobs), Q}.

dropwhile(Q, NotJobs) ->
        case queue:out(Q) of
                {{value, Job}, NQ} ->
                        V = lists:member(Job, NotJobs),
                        if V ->
                                dropwhile(NQ, NotJobs);
                        true ->
                                {ok, Job}
                        end;
                {empty, _} -> nojobs
        end.

handle_info({'DOWN', _, _, Job, _}, Q) ->
        {noreply, queue:filter(fun(J) -> J =/= Job end, Q)}.

% unused

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
