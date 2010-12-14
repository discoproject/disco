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

json_list(List) -> json_list(List, []).
json_list([], _) -> [];
json_list([X], L) ->
    [<<"[">>, lists:reverse([X|L]), <<"]">>];
json_list([X|R], L) ->
    json_list(R, [<<X/binary, ",">>|L]).

-spec unique_key(nonempty_string(), dict()) -> nonempty_string().
unique_key(Prefix, Dict) ->
    {MegaSecs, Secs, MicroSecs} = now(),
    Key = lists:flatten(io_lib:format("~s@~.16b:~.16b:~.16b", [Prefix, MegaSecs, Secs, MicroSecs])),
    case dict:is_key(Key, Dict) of
        false ->
            Key;
        true ->
            unique_key(Prefix, Dict)
    end.

handle_call(get_jobs, _From, {Events, _MsgBuf} = S) ->
    Jobs = dict:fold(fun
        (Name, {[{ready, _Results}|_], Start, Pid}, Acc) ->
            [{list_to_binary(Name), ready, Start, Pid}|Acc];
        (Name, {_Events, Start, Pid}, Acc) ->
            [{list_to_binary(Name), process_status(Pid), Start, Pid}|Acc]
    end, [], Events),
    {reply, {ok, Jobs}, S};

handle_call({get_job_events, JobName, Query, N0}, _From, {_Events, MsgBuf} = S) ->
    N = if
            N0 > 1000 -> 1000;
            true -> N0
        end,
    case dict:find(JobName, MsgBuf) of
        _ when Query =/= "" ->
            {reply, {ok,
                 json_list(grep_log(JobName, Query, N))}, S};
        {ok, {_NMsg, _ListLength, MsgLst}} ->
            {reply, {ok,
                 json_list(lists:sublist(MsgLst, N))}, S};
        error ->
            {reply, {ok,
                 json_list(tail_log(JobName, N))}, S}
    end;

handle_call({get_results, JobName}, _From, {Events, _MsgBuf} = S) ->
    case dict:find(JobName, Events) of
        error ->
            {reply, invalid_job, S};
        {ok, {[{ready, Results}|_EventList], _JobStart, Pid}} ->
            {reply, {ready, Pid, Results}, S};
        {ok, {_EventList, _JobStart, Pid}} ->
            {reply, {process_status(Pid), Pid}, S}
    end;

handle_call({get_map_results, JobName}, _From, {Events, _MsgBuf} = S) ->
    case dict:find(JobName, Events) of
        error ->
            {reply, invalid_job, S};
        {ok, {EventList, _JobStart, _Pid}} ->
            case event_filter(map_ready, EventList) of
                [] -> {reply, not_ready, S};
                [Res] -> {reply, {ok, Res}, S}
            end
    end;

handle_call({new_job, JobPrefix, Pid}, From, {Events0, MsgBuf0}) ->
    JobName = unique_key(JobPrefix, Events0),
    Events = dict:store(JobName, {[], now(), Pid}, Events0),
    MsgBuf = dict:store(JobName, {0, 0, []}, MsgBuf0),
    spawn(fun() -> job_event_handler(JobName, From) end),
    {noreply, {Events, MsgBuf}};

handle_call({job_initialized, JobName, JobEventHandler}, _From, S) ->
    ets:insert(event_files, {JobName, JobEventHandler}),
    {reply, add_event("master", JobName, list_to_binary("\"New job initialized!\""), {}, S), S};

handle_call({get_jobinfo, JobName}, _From, {Events, _MsgBuf} = S) ->
    case dict:find(JobName, Events) of
        error ->
            {reply, invalid_job, S};
        {ok, {EventList, JobStart, Pid}} ->
            JobNfo =
                case event_filter(job_data, EventList) of
                    [] -> [];
                    [N] -> N
                end,
            Results = event_filter(ready, EventList),
            Ready = event_filter(task_ready, EventList),
            Failed = event_filter(task_failed, EventList),
            Start = format_timestamp(JobStart),
            {reply, {ok, {Start, Pid, JobNfo, Results, Ready, Failed}}, S}
    end.

handle_cast({add_job_event, Host, JobName, Msg, Params}, {_Events, MsgBuf} = S) ->
    case dict:is_key(JobName, MsgBuf) of
        true  -> {noreply, add_event(Host, JobName, Msg, Params, S)};
        false -> {noreply, S}
    end;

% XXX: Some aux process could go through the jobs periodically and
% check that the job coord is still alive - if not, call job_done for
% the zombie job.
handle_cast({job_done, JobName}, {Events, MsgBuf} = S) ->
    case ets:lookup(event_files, JobName) of
        [] ->
            ok;
        [{_, EventProc}] ->
            EventProc ! done,
            ets:delete(event_files, JobName)
    end,
    case dict:find(JobName, MsgBuf) of
        error ->
            {noreply, S};
        {ok, _} ->
            {noreply, {Events, dict:erase(JobName, MsgBuf)}}
    end;

handle_cast({clean_job, JobName}, {Events, _MsgBuf} = S) ->
    {_, {_, MsgBufN}} = handle_cast({job_done, JobName}, S),
    delete_jobdir(JobName),
    {noreply, {dict:erase(JobName, Events), MsgBufN}}.

handle_info(Msg, State) ->
    error_logger:warning_report(["Unknown message received: ", Msg]),
    {noreply, State}.

tail_log(JobName, N)->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    FName = filename:join([Root, disco:jobhome(JobName), "events"]),
    Tail = string:tokens(os:cmd(["tail -n ", integer_to_list(N),
                     " ", FName, " 2>/dev/null"]), "\n"),
    [list_to_binary(L) || L <- lists:reverse(Tail)].

grep_log(JobName, Query, N) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    FName = filename:join([Root, disco:jobhome(JobName), "events"]),

    % We dont want execute stuff like "grep -i `rm -Rf *` ..." so
    % only whitelisted characters are allowed in the query
    {ok, CQ, _} = regexp:gsub(Query, "[^a-zA-Z0-9:-_!@]", ""),
    Lines = string:tokens(os:cmd(["grep -i \"", CQ ,"\" ", FName,
                      " 2>/dev/null | head -n ", integer_to_list(N)]), "\n"),
    lists:map(fun erlang:list_to_binary/1, lists:reverse(Lines)).

process_status(Pid) ->
    case is_process_alive(Pid) of
        true -> active;
        false -> dead
    end.

event_filter(Key, EventList) ->
    [V || {K, V} <- EventList, K == Key].

format_timestamp(TimeStamp) ->
    {Date, Time} = calendar:now_to_local_time(TimeStamp),
    DateStr = io_lib:fwrite("~w/~.2.0w/~.2.0w ", tuple_to_list(Date)),
    TimeStr = io_lib:fwrite("~.2.0w:~.2.0w:~.2.0w", tuple_to_list(Time)),
    list_to_binary([DateStr, TimeStr]).

add_event(Host0, JobName, Msg, Params, {Events, MsgBuf}) ->
    {ok, {NMsg, LstLen0, MsgLst0}} = dict:find(JobName, MsgBuf),
    Time = format_timestamp(now()),
    Host = list_to_binary(Host0),
    Line = <<"[\"",
        Time/binary, "\",\"",
        Host/binary, "\",",
        Msg/binary, "]", 10>>,

    [{_, EventProc}] = ets:lookup(event_files, JobName),
    EventProc ! {event, Line},

    if
        LstLen0 + 1 > ?EVENT_PAGE_SIZE * 2 ->
            MsgLst = lists:sublist([Line|MsgLst0], ?EVENT_PAGE_SIZE),
            LstLen = ?EVENT_PAGE_SIZE;
        true ->
            MsgLst = [Line|MsgLst0],
            LstLen = LstLen0 + 1
    end,
    MsgBufN = dict:store(JobName, {NMsg + 1, LstLen, MsgLst}, MsgBuf),
    if
        Params == [] ->
            {Events, MsgBufN};
        true ->
            {ok, {EvLst0, Nu, Pid}} = dict:find(JobName, Events),
            {dict:store(JobName, {[Params|EvLst0], Nu, Pid}, Events),
             MsgBufN}
    end.

%-spec event(nonempty_string(), nonempty_string(), [_], [_]) -> _.
event(JobName, Format, Args, Params) ->
    event("master", JobName, Format, Args, Params).

%-spec event(nonempty_string(), nonempty_string(), nonempty_string(), [_], [_]) -> _.
event(Host, JobName, Format, Args, Params) ->
    event(event_server, Host, JobName, Format, Args, Params).

%-spec event(atom(), nonempty_string(), nonempty_string(), nonempty_string(), [_], [_]) -> _.
event(EventServer, Host, JobName, Format, Args, Params) ->
    SArgs = [case lists:flatlength(io_lib:fwrite("~p", [X])) > 10000 of
		     true -> trunc_io:fprint(X, 10000);
		     false -> X 
		 end || X <- Args],
    RawMsg = lists:flatten(io_lib:fwrite(Format, SArgs)),
    Json = case catch mochijson2:encode(list_to_binary(RawMsg)) of
               {'EXIT', _} ->
                   Hex = ["WARNING: Binary message data: ",
                          [io_lib:format("\\x~2.16.0b",[N])
                           || N <- RawMsg]],
                   mochijson2:encode(list_to_binary(Hex));
               J -> J
           end,
    Msg = list_to_binary(Json),
    gen_server:cast(EventServer, {add_job_event, Host, JobName, Msg, Params}).

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

check_mkdir(ok) -> ok;
check_mkdir({error, eexist}) -> ok;
check_mkdir(_) -> throw("creating directory failed").

prepare_environment(Name) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    Home = disco:jobhome(Name),
    [Path, _] = filename:split(Home),
    check_mkdir(file:make_dir(filename:join(Root, Path))),
    check_mkdir(file:make_dir(filename:join(Root, Home))).

delete_jobdir(Name) ->
    Safe = string:chr(Name, $.) + string:chr(Name, $/),
    if Safe =:= 0 ->
        Root = disco:get_setting("DISCO_MASTER_ROOT"),
        Home = disco:jobhome(Name),
        os:cmd("rm -Rf " ++ filename:join(Root, Home));
    true -> ok
    end.

job_event_handler(JobName, JobCoordinator) ->
    prepare_environment(JobName),
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    FName = filename:join([Root, disco:jobhome(JobName), "events"]),
    {ok, File} = file:open(FName, [append, raw]),
    gen_server:call(event_server, {job_initialized, JobName, self()}),
    gen_server:reply(JobCoordinator, {ok, JobName}),
    timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
    job_event_handler_do(File, [], 0).

job_event_handler_do(File, Buf, BufSize) when BufSize > ?EVENT_BUFFER_SIZE ->
    flush_buffer(File, Buf),
    job_event_handler_do(File, [], 0);
job_event_handler_do(File, Buf, BufSize) ->
    receive
        {event, Line} ->
            job_event_handler_do(File, [Line|Buf], BufSize + 1);
        flush ->
            flush_buffer(File, Buf),
            timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
            job_event_handler_do(File, [], 0);
        done ->
            flush_buffer(File, Buf),
            file:close(File);
        E ->
            error_logger:warning_report(
                {"Unknown message in job_event_handler", E}),
            file:close(File)
    end.

flush_buffer(_, []) -> ok;
flush_buffer(File, Buf) ->
    file:write(File, lists:reverse(Buf)).
