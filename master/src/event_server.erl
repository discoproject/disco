-module(event_server).
-behaviour(gen_server).

-define(EVENT_PAGE_SIZE, 1000).

-export([format_timestamp/1, event/4, event/5, event/6]).
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
        {ok, Root} = application:get_env(disco_root),
        Pid = spawn_link(fun() -> log_flusher_process(Root) end),
        register(event_flusher, Pid),
        {ok, {dict:new(), dict:new()}}.

handle_call(get_jobnames, _From, {Events, _} = S) ->
        Lst = dict:fold(fun(JobName, {_, Nu, Pid}, L) ->
                [{JobName, Nu, Pid}|L]
        end, [], Events),
        {reply, {ok, Lst}, S};

handle_call({get_job_events, JobName, Q, N0}, _From, {_, MsgBuf} = S) ->
        N = if N0 > 1000 -> 1000; true -> N0 end,
        case dict:find(JobName, MsgBuf) of
                error -> {reply, {ok, tail_log(JobName, N)}, S};
                {ok, {_, _, MsgLst}} when Q == "" ->
                        {reply, {ok, lists:sublist(MsgLst, N)}, S};
                {ok, D} ->
                        {reply, {ok, grep_log(JobName, D, Q, N)}, S}
        end;

handle_call({get_results, JobName}, _From, {Events, _} = S) ->
        case dict:find(JobName, Events) of
                error -> {reply, invalid_job, S};
                {ok, {[{ready, Res}|_], _, Pid}} ->
                        {reply, {ok, Pid, Res}, S};
                {ok, {_, _, Pid}} ->
                        {reply, {ok, Pid}, S}
        end;
                
handle_call({get_jobinfo, JobName}, _From, {Events, _} = S) ->
        case dict:find(JobName, Events) of
                error -> {reply, invalid_job, S};
                {ok, {EvLst, Nu, Pid}} ->
                        JobNfo = event_filter(job_data, EvLst),
                        Res = event_filter(ready, EvLst),
                        Ready = event_filter(task_ready, EvLst),
                        Failed = event_filter(task_failed, EvLst),
                        TStamp = list_to_binary(format_timestamp(Nu)),
                        {reply, {ok, {TStamp, Pid, 
                                JobNfo, Res, Ready, Failed}}, S}
        end.

% There's a small catch with adding a job event: If a job's records have been
% cleaned with clean_job already, we do not want a zombie worker to re-open
% the job's records by adding a new event. Thus only an event with the atom
% start is allowed to initialize records for a new job. Other events are 
% silently ignored if there are no previous records for this job.
handle_cast({add_job_event, Host, JobName, M, {start, Pid} = P},
        {Events0, MsgBuf0} = S) ->
        
        V = dict:is_key(JobName, Events0),
        if V -> {noreply, S};
        true ->
                Events = dict:store(JobName, {[], now(), Pid}, Events0),
                MsgBuf = dict:store(JobName, {0, 0, []}, MsgBuf0),
                {noreply, add_event(Host, JobName, M, P, {Events, MsgBuf})}
        end;

handle_cast({add_job_event, Host, JobName, M, P}, {_, MsgBuf} = S) ->
        V = dict:is_key(JobName, MsgBuf),
        if V -> {noreply, add_event(Host, JobName, M, P, S)};
        true -> {noreply, S}
        end;

handle_cast({flush_events, JobName}, {Events, MsgBuf} = S) ->
        case dict:find(JobName, MsgBuf) of
                error -> {noreply, S};
                {ok, {_, _, MsgLst}} ->
                        flush_msgbuf(JobName, MsgLst),
                        {noreply, {Events,
                                dict:erase(JobName, MsgBuf)}}
        end;

handle_cast({clean_job, JobName}, {Events, _} = S) ->
        {_, {_, MsgBufN}} = handle_cast({flush_events, JobName}, S),
        {noreply, {dict:erase(JobName, Events), MsgBufN}}.

handle_info(Msg, State) ->
        error_logger:info_report(["Unknown message received: ", Msg]),
        {noreply, State}.

tail_log(JobName, N)->
        {ok, Root} = application:get_env(disco_root),
        FName = filename:join([Root, JobName, "events"]),
        O = string:tokens(os:cmd(["tail -n ", integer_to_list(N),
                " ", FName, " 2>/dev/null"]), "\n"),
        lists:map(fun erlang:list_to_binary/1, lists:reverse(O)).

grep_log(JobName, {_, _, MsgLst}, Q, N) ->
        {ok, Root} = application:get_env(disco_root),
        FName = filename:join([Root, JobName, "events"]),
        M = lists:filter(fun(E) ->
                 string:str(string:to_lower(binary_to_list(E)), Q) > 0
        end, MsgLst),
        % We dont want execute stuff like "grep -i `rm -Rf *` ..." so
        % only whitelisted characters are allowed in the query
        {ok, CQ, _} = regexp:gsub(Q, "[^a-z0-9:-_!@]", ""),
        O = string:tokens(os:cmd(["grep -i \"", CQ ,"\" ", FName,
                " 2>/dev/null | head -n ", integer_to_list(N)]), "\n"),
        M ++ lists:map(fun erlang:list_to_binary/1, lists:reverse(O)).

event_filter(Key, EvLst) ->
        {_, R} = lists:unzip(lists:filter(fun
                ({K, _}) when K == Key -> true;
                (_) -> false
        end, EvLst)), R.

flush_msgbuf(JobName, FlushBuf) ->
        event_flusher ! {flush, JobName, FlushBuf}.

log_flusher_process(Root) ->
        receive 
                {flush, JobName, FlushBuf} ->
                        FName = filename:join([Root, JobName, "events"]),
                        {ok, F} = file:open(FName, [raw, append]),
                        file:write(F, lists:reverse(FlushBuf)),
                        file:close(F),
                        log_flusher_process(Root)
        end.

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        DateStr ++ TimeStr.

add_event(Host, JobName, Msg, Params, {Events, MsgBuf}) ->
        {ok, {NMsg, LstLen0, MsgLst0}} = dict:find(JobName, MsgBuf),
        M = iolist_to_binary([format_timestamp(now()), $@, Host, $;, Msg]),

        if LstLen0 + 1 > ?EVENT_PAGE_SIZE * 2 ->
                {MsgLst, FlushBuf} = lists:split(?EVENT_PAGE_SIZE,
                        [<<M/binary, 10>>|MsgLst0]),
                flush_msgbuf(JobName, FlushBuf),
                LstLen = ?EVENT_PAGE_SIZE;
        true ->
                MsgLst = [<<M/binary, 10>>|MsgLst0],
                LstLen = LstLen0 + 1
        end,
        MsgBufN = dict:store(JobName, {NMsg + 1, LstLen, MsgLst}, MsgBuf),
        if Params == [] -> 
                {Events, MsgBufN};
        true ->
                {ok, {EvLst0, Nu, Pid}} = dict:find(JobName, Events),
                {dict:store(JobName, {[Params|EvLst0], Nu, Pid}, Events),
                        MsgBufN}
        end.

event(JobName, Format, Args, Params) ->
        event("master", JobName, Format, Args, Params).

event(Host, JobName, Format, Args, Params) ->
        event(event_server, Host, JobName, Format, Args, Params).

event(EventServ, Host, JobName, Format, Args, Params) ->
        SArgs = lists:map(fun(X) ->
                L = lists:flatlength(io_lib:fwrite("~p", [X])) > 1000,
                if L -> trunc_io:fprint(X, 1000);
                true -> X 
        end end, Args),

        gen_server:cast(EventServ, {add_job_event, Host, JobName,
                lists:flatten(io_lib:fwrite(Format, SArgs)), Params}).
        
% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

