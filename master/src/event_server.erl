-module(event_server).
-behaviour(gen_server).

-define(EVENT_PAGE_SIZE, 100).
-define(EVENT_BUFFER_SIZE, 1000).
-define(EVENT_BUFFER_TIMEOUT, 2000).

-export([event/4, event/5, event/6]).
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
        ets:new(event_files, [named_table]),
        {ok, {dict:new(), dict:new()}}.

json_list([], _) -> [];
json_list([X], L) ->
        [<<"[">>, lists:reverse([X|L]), <<"]">>];
json_list([X|R], L) ->
        json_list(R, [<<X/binary, ",">>|L]).

handle_call(get_jobnames, _From, {Events, _} = S) ->
        Lst = dict:fold(fun(JobName, {_, Nu, Pid}, L) ->
                [{JobName, Nu, Pid}|L]
        end, [], Events),
        {reply, {ok, Lst}, S};

handle_call({get_job_events, JobName, Q, N0}, _From, {_, MsgBuf} = S) ->
        N = if N0 > 1000 -> 1000; true -> N0 end,
        case dict:find(JobName, MsgBuf) of
                _ when Q =/= "" ->
                        {reply, {ok, 
                                json_list(grep_log(JobName, Q, N), [])}, S};
                {ok, {_, _, MsgLst}} ->
                        {reply, {ok,
                                json_list(lists:sublist(MsgLst, N), [])}, S};
                error ->
                        {reply, {ok,
                                json_list(tail_log(JobName, N), [])}, S}
        end;

handle_call({get_results, JobName}, _From, {Events, _} = S) ->
        case dict:find(JobName, Events) of
                error -> {reply, invalid_job, S};
                {ok, {[{ready, Res}|_], _, Pid}} ->
                        {reply, {ready, Pid, Res}, S};
                {ok, {_, _, Pid}} ->
                        Alive = is_process_alive(Pid),
                        if Alive ->
                                {reply, {active, Pid}, S};
                        true ->
                                {reply, {dead, Pid}, S}
                        end
        end;

handle_call({new_job, JobName, Pid}, _From, {Events0, MsgBuf0} = S) ->
        V = dict:is_key(JobName, Events0),
        if V -> {reply, eexist, S};
        true ->
                Events = dict:store(JobName, {[], now(), Pid}, Events0),
                MsgBuf = dict:store(JobName, {0, 0, []}, MsgBuf0),
                
                {ok, Root} = application:get_env(disco_root),
                FName = filename:join([Root, disco_server:jobhome(JobName), "events"]),
                EventProc = spawn(fun() -> job_event_handler(FName) end),
                ets:insert(event_files, {JobName, EventProc}),
                
                {reply, ok, add_event("master", JobName, list_to_binary("\"New job!\""),
                        {start, Pid}, {Events, MsgBuf})}
        end;

handle_call({get_jobinfo, JobName}, _From, {Events, _} = S) ->
        case dict:find(JobName, Events) of
                error -> {reply, invalid_job, S};
                {ok, {EvLst, Nu, Pid}} ->
                        JobNfo = event_filter(job_data, EvLst),
                        Res = event_filter(ready, EvLst),
                        Ready = event_filter(task_ready, EvLst),
                        Failed = event_filter(task_failed, EvLst),
                        TStamp = format_timestamp(Nu),
                        {reply, {ok, {TStamp, Pid, 
                                JobNfo, Res, Ready, Failed}}, S}
        end.


handle_cast({add_job_event, Host, JobName, M, P}, {_, MsgBuf} = S) ->
        V = dict:is_key(JobName, MsgBuf),
        if V -> {noreply, add_event(Host, JobName, M, P, S)};
        true -> {noreply, S}
        end;

handle_cast({job_done, JobName}, {Events, MsgBuf} = S) ->
        case ets:lookup(event_files, JobName) of
                [] -> ok;
                [{_, EventProc}] ->
                        EventProc ! done,
                        ets:delete(event_files, JobName)
        end,
        case dict:find(JobName, MsgBuf) of
                error -> {noreply, S};
                {ok, _} ->
                        {noreply, {Events, dict:erase(JobName, MsgBuf)}}
        end;

handle_cast({clean_job, JobName}, {Events, _} = S) ->
        {_, {_, MsgBufN}} = handle_cast({job_done, JobName}, S),
        {noreply, {dict:erase(JobName, Events), MsgBufN}}.

handle_info(Msg, State) ->
        error_logger:info_report(["Unknown message received: ", Msg]),
        {noreply, State}.

tail_log(JobName, N)->
        {ok, Root} = application:get_env(disco_root),
        FName = filename:join([Root, disco_server:jobhome(JobName), "events"]),
        O = string:tokens(os:cmd(["tail -n ", integer_to_list(N),
                " ", FName, " 2>/dev/null"]), "\n"),
        lists:map(fun erlang:list_to_binary/1, lists:reverse(O)).

grep_log(JobName, Q, N) ->
        {ok, Root} = application:get_env(disco_root),
        FName = filename:join([Root, disco_server:jobhome(JobName), "events"]),
        
        % We dont want execute stuff like "grep -i `rm -Rf *` ..." so
        % only whitelisted characters are allowed in the query
        {ok, CQ, _} = regexp:gsub(Q, "[^a-zA-Z0-9:-_!@]", ""),
        O = string:tokens(os:cmd(["grep -i \"", CQ ,"\" ", FName,
                " 2>/dev/null | head -n ", integer_to_list(N)]), "\n"),
        lists:map(fun erlang:list_to_binary/1, lists:reverse(O)).

event_filter(Key, EvLst) ->
        {_, R} = lists:unzip(lists:filter(fun
                ({K, _}) when K == Key -> true;
                (_) -> false
        end, EvLst)), R.

format_timestamp(Tstamp) ->
        {Date, Time} = calendar:now_to_local_time(Tstamp),
        DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
        TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
        list_to_binary([DateStr, TimeStr]).

add_event(Host, JobName, Msg, Params, {Events, MsgBuf}) ->
        {ok, {NMsg, LstLen0, MsgLst0}} = dict:find(JobName, MsgBuf),
        T = format_timestamp(now()),
        H = list_to_binary(Host),
        Line = <<"[\"", T/binary, "\",\"", H/binary, "\",", Msg/binary, "]", 10>>,

        [{_, EventProc}] = ets:lookup(event_files, JobName),
        EventProc ! {event, Line},

        if LstLen0 + 1 > ?EVENT_PAGE_SIZE * 2 ->
                MsgLst = lists:sublist([Line|MsgLst0], ?EVENT_PAGE_SIZE),
                LstLen = ?EVENT_PAGE_SIZE;
        true ->
                MsgLst = [Line|MsgLst0],
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

        M = list_to_binary(json:encode(
                list_to_binary(io_lib:fwrite(Format, SArgs)))),
        gen_server:cast(EventServ, {add_job_event, Host, JobName, M, Params}).
        
% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

job_event_handler(FName) ->
        {ok, F} = file:open(FName, [append, raw]),
        timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
        job_event_handler_do(F, [], 0). 

job_event_handler_do(F, Buf, BufSize) when BufSize > ?EVENT_BUFFER_SIZE ->
        flush_buffer(F, Buf),
        job_event_handler_do(F, [], 0);

job_event_handler_do(F, Buf, BufSize) ->
        receive
                {event, Line} ->
                        job_event_handler_do(F, [Line|Buf], BufSize + 1);
                flush ->
                        flush_buffer(F, Buf),
                        timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
                        job_event_handler_do(F, [], 0);
                done ->
                        flush_buffer(F, Buf),
                        file:close(F);
                E ->
                        error_logger:warning_report(
                                {"Unknown message in job_event_handler", E}),
                        file:close(F)
        end.

flush_buffer(_, []) -> ok;
flush_buffer(F, Buf) ->
        file:write(F, lists:reverse(Buf)).
                        
                        
                
        
        
