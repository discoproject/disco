% This module implements a scheduler policy for the global task
% scheduler (GTS) in fair_scheduler.erl.  This module implements a
% FIFO scheduling policy, where new jobs are added to the end of a
% queue, and the job at the head of the queue is always selected as
% the scheduling candidate.  Jobs only leave the queue when they
% complete or die.

-module(fair_scheduler_fifo_policy).
-behaviour(gen_server).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("fair_scheduler.hrl").

-export([start_link/0, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-type state() :: queue().

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Fair scheduler: FIFO policy"),
    case gen_server:start_link({local, sched_policy},
                               fair_scheduler_fifo_policy, [],
                               disco:debug_flags("fair_scheduler_fifo_policy"))
    of  {ok, _Server} = Ret -> Ret;
        {error, {already_started, Server}} -> {ok, Server}
    end.

-spec init(_) -> gs_init().
init(_) ->
    {ok, queue:new()}.

-type cast_msgs() :: policy_cast_msgs().

-spec handle_cast(cast_msgs(), state()) -> gs_noreply().
handle_cast({update_nodes, _}, Q) ->
    {noreply, Q};
handle_cast({new_job, JobPid, JobName}, Q) ->
    erlang:monitor(process, JobPid),
    {noreply, queue:in({JobPid, JobName}, Q)}.


-spec handle_call(current_priorities_msg(), from(), state()) ->
                         gs_reply([{jobname(), priority()}]);
                 (dbg_state_msg(), from(), state()) -> gs_reply(state());
                 (next_job_msg(), from(), state()) -> gs_reply(next_job()).
handle_call(current_priorities, _, Q) ->
    {reply, {ok, case queue:to_list(Q) of
                     [{_, N}|R] -> [{N, -1.0}|[{M, 1.0} || {_, M} <- R]];
                     []         -> []
                 end}, Q};

handle_call(dbg_get_state, _, Q) ->
    {reply, Q, Q};

handle_call({next_job, NotJobs}, _, Q) ->
    {reply, dropwhile(Q, NotJobs), Q}.

-spec dropwhile(state(), [pid()]) -> next_job().
 dropwhile(Q, NotJobs) ->
    case queue:out(Q) of
        {{value, {JobPid, _}}, NQ} ->
            V = lists:member(JobPid, NotJobs),
            if V    -> dropwhile(NQ, NotJobs);
               true -> {ok, JobPid}
            end;
        {empty, _} -> nojobs
    end.

-spec handle_info({'DOWN', _, _, pid(), _}, state()) -> gs_noreply().
handle_info({'DOWN', _, _, JobPid, _}, Q) ->
    L = queue:to_list(Q),
    {value, {_, JobName} = E} = lists:keysearch(JobPid, 1, L),
    fair_scheduler:job_done(JobName),
    {noreply, queue:from_list(L -- [E])}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
