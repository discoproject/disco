-module(event_server).
-behaviour(gen_server).

-export([format_timestamp/1, event/4, event/5]).
-export([start_link/0, stop/0, init/1, handle_call/3, handle_cast/2, 
        handle_info/2, terminate/2, code_change/3]).

start_link() ->
        error_logger:info_report([{"Event server starts"}]),
        case gen_server:start_link({local, event_server}, event_server, [], []) of
                {ok, Server} -> {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

stop() ->
        gen_server:call(event_server, stop).

init(_Args) ->
        % job_events records all events related to a job
        ets:new(job_events, [named_table, duplicate_bag]),
        {ok, none}.


handle_call(get_jobnames, _From, State) ->
        Lst = ets:match(job_events, 
                {'$1', {{'$2', '_'}, '_', ['_', start, '$3'|'_']}}),
        {reply, {ok, Lst}, State};

handle_call({get_job_events, JobName}, _From, State) ->
        case ets:lookup(job_events, JobName) of
                [] -> {reply, {ok, []}, State};
                Events -> {_, EventList} = lists:unzip(Events),
                          {reply, {ok, EventList}, State}
        end;

handle_call({get_results, JobName}, _From, State) ->
        Pid = lists:flatten(ets:match(job_events,
                {JobName, {'_', '_', ['_', map_data, '$1'|'_']}})),
        Res = lists:flatten(ets:match(job_events,
                {JobName, {'_', '_', ['_', ready|'$1']}})),
        {reply, {ok, Pid, Res}, State};

handle_call({get_jobinfo, JobName}, _From, State) ->
        MapNfo = ets:match(job_events,
                {JobName, {{'_', '$1'}, '_', ['_', map_data|'$2']}}),
        
        Res = ets:match(job_events, {JobName, {'_', '_', ['_', ready|'$1']}}),
        Ready = ets:match(job_events,
                {JobName, {'_', '_', ['_', task_ready|'$1']}}),
        Failed = ets:match(job_events,
                {JobName, {'_', '_', ['_', task_failed|'$1']}}),

        {reply, {ok, {MapNfo, Res, Ready, Failed}}, State}.

% There's a small catch with adding a job event: If a job's records have been
% cleaned with clean_job already, we do not want a zombie worker to re-open
% the job's records by adding a new event. Thus only an event with the atom
% start is allowed to initialize records for a new job. Other events are 
% silently ignored if there are no previous records for this job.
handle_cast({add_job_event, Host, JobName, [_, start|_] = M}, State) ->
        V = ets:member(job_events, JobName),
        if V -> {noreply, State};
        true -> add_event(Host, JobName, M),
                {noreply, State}
        end;

handle_cast({add_job_event, Host, JobName, M}, State) ->
        V = ets:member(job_events, JobName),
        if V -> add_event(Host, JobName, M);
        true -> ok end,
        {noreply, State};

handle_cast({clean_job, JobName}, State) ->
        ets:delete(job_events, JobName),
        {noreply, State}.

handle_info(Msg, State) ->
        error_logger:info_report(["Unknown message received: ", Msg]),
        {noreply, State}.

% Functions related to event reporting

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        DateStr ++ TimeStr.

add_event(Host, JobName, [Msg|Params]) ->
        Nu = now(),
        ets:insert(job_events, {JobName, {
                {Nu, list_to_binary(format_timestamp(Nu))},
                list_to_binary(Host), 
                [list_to_binary(Msg)|Params]
        }}).

event(JobName, Format, Args, Params) ->
        event("master", JobName, Format, Args, Params).

event(Host, JobName, Format, Args, Params) ->
        SArgs = lists:map(fun(X) ->
                L = lists:flatlength(io_lib:fwrite("~p", [X])) > 1000,
                if L -> trunc_io:fprint(X, 1000);
                true -> X 
        end end, Args),

        Msg = {add_job_event, Host, JobName,
                [lists:flatten(io_lib:fwrite(Format, SArgs))|Params]},
        gen_server:cast(event_server, Msg).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

