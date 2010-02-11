-module(job_queue).
-behaviour(gen_server).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, 
    handle_info/2, terminate/2, code_change/3]).

start_link() ->
    error_logger:info_report([{"Job queue starts"}]),
    case gen_server:start_link({local, job_queue}, job_queue, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

init(_Args) ->
    {ok, queue:new()}.

handle_cast({add_job, Job}, Queue) ->
    E = queue:is_empty(Queue),
    if E ->
        {noreply, schedule_job({value, Job}, Queue, queue:new())};
    true ->
        {noreply, queue:in(Job, Queue)}
    end;    

handle_cast(schedule_job, Queue) ->
    {Job, Rest} = queue:out(Queue),
    {noreply, schedule_job(Job, Rest, queue:new())};

handle_cast({filter_queue, Fun}, Queue) ->
    Q = queue:from_list(lists:filter(Fun, queue:to_list(Queue))),
    {noreply, Q}.

schedule_job(empty, _Rest, Skipped) -> Skipped;
schedule_job({value, Job}, Rest, Skipped) ->
    case gen_server:call(disco_server, {try_new_worker, Job}) of
        ok ->
            queue:join(Skipped, Rest);
        killed ->
            queue:join(Skipped, Rest);
        {wait, busy} ->
            queue:join(queue:in(Job, Skipped), Rest);
        {wait, all_bad} ->
            {NJob, NRest} = queue:out(Rest),
            schedule_job(NJob, NRest, queue:in(Job, Skipped))
    end.

% unused

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}. 

terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
