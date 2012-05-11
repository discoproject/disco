-module(event_server).
-behaviour(gen_server).

-define(EVENT_PAGE_SIZE, 100).
-define(EVENT_BUFFER_SIZE, 1000).
-define(EVENT_BUFFER_TIMEOUT, 2000).

-export([new_job/2,
         end_job/1,
         event/4,
         event/5,
         event/6,
         task_event/2,
         task_event/3,
         task_event/4,
         task_event/5]).
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").
-include("gs_util.hrl").

-spec new_job(jobname(), pid()) -> {ok, jobname()}.
new_job(Prefix, JobCoordinator) ->
    gen_server:call(?MODULE, {new_job, Prefix, JobCoordinator}, 10000).

-spec end_job(jobname()) -> ok.
end_job(JobName) ->
    gen_server:cast(?MODULE, {job_done, JobName}).

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Event server starts"),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

% state :: {events dict, msgbuf dict}.
% events dict: jobname -> { [<event>], <start_time>, <job_coordinator_pid> }
%              event ::= {ready, <results>}
%                      | {map_ready, <map-results>}
%                      | {reduce_ready, <reduce-results>}
%                      | {job_data, jobinfo()}
%                      | {task_ready, "map" | "reduce"}
%                      | {task_failed, "map" | "reduce"}
%
% msgbuf dict: jobname -> { <nmsgs>, <list-length>, [<msg>] }
%

-type state() :: {dict(), dict()}.

-spec init(_) -> gs_init().
init(_Args) ->
    _ = ets:new(event_files, [named_table]),
    {ok, {dict:new(), dict:new()}}.

json_list(List) -> json_list(List, []).
json_list([], _) -> [];
json_list([X], L) ->
    [<<"[">>, lists:reverse([X|L]), <<"]">>];
json_list([X|R], L) ->
    json_list(R, [<<X/binary, ",">>|L]).

-spec unique_key(jobname(), dict()) -> invalid_prefix | {ok, jobname()}.
unique_key(Prefix, Dict) ->
    C = string:chr(Prefix, $/) + string:chr(Prefix, $.),
    if C > 0 ->
        invalid_prefix;
    true ->
        {MegaSecs, Secs, MicroSecs} = now(),
        Key = disco:format("~s@~.16b:~.16b:~.16b", [Prefix, MegaSecs, Secs, MicroSecs]),
        case dict:is_key(Key, Dict) of
            false ->
                {ok, Key};
            true ->
                unique_key(Prefix, Dict)
        end
    end.

-spec handle_call(term(), from(), state()) -> gs_reply(term()) | gs_noreply().

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

handle_call({new_job, JobPrefix, Pid}, From, {Events0, MsgBuf0} = S) ->
    case unique_key(JobPrefix, Events0) of
        invalid_prefix ->
            {reply, {error, invalid_prefix}, S};
        {ok, JobName} ->
            Events = dict:store(JobName, {[], now(), Pid}, Events0),
            MsgBuf = dict:store(JobName, {0, 0, []}, MsgBuf0),
            spawn(fun() -> job_event_handler(JobName, From) end),
            {noreply, {Events, MsgBuf}}
    end;

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
            Start = disco_util:format_timestamp(JobStart),
            {reply, {ok, {Start, Pid, JobNfo, Results, Ready, Failed}}, S}
    end.

-spec handle_cast(term(), state()) -> gs_noreply().

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

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(Msg, State) ->
    lager:warning("Unknown message received: ~p", [Msg]),
    {noreply, State}.

event_log(JobName) ->
    filename:join(disco:jobhome(JobName), "events").

tail_log(JobName, N) ->
    Tail = string:tokens(os:cmd(["tail -n ", integer_to_list(N), " ",
                                 event_log(JobName),
                                 " 2>/dev/null"]), "\n"),
    [list_to_binary(L) || L <- lists:reverse(Tail)].

grep_log(JobName, Query, N) ->
    % We dont want execute stuff like "grep -i `rm -Rf *` ..." so
    % only whitelisted characters are allowed in the query
    CQ = re:replace(Query, "[^a-zA-Z0-9:-_!@]", "", [global, {return, list}]),
    Lines = string:tokens(os:cmd(["grep -i \"", CQ ,"\" ",
                                  event_log(JobName),
                                  " 2>/dev/null | head -n ", integer_to_list(N)]), "\n"),
    [list_to_binary(L) || L <- lists:reverse(Lines)].

process_status(Pid) ->
    case is_process_alive(Pid) of
        true -> active;
        false -> dead
    end.

event_filter(Key, EventList) ->
    [V || {K, V} <- EventList, K == Key].

add_event(Host0, JobName, Msg, Params, {Events, MsgBuf}) ->
    {ok, {NMsg, LstLen0, MsgLst0}} = dict:find(JobName, MsgBuf),
    Time = disco_util:format_timestamp(now()),
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

-spec event(jobname(), nonempty_string(), list(), tuple()) -> ok.
event(JobName, Format, Args, Params) ->
    event("master", JobName, Format, Args, Params).

-spec event(host_name(), jobname(), nonempty_string(), list(), tuple()) -> ok.
event(Host, JobName, Format, Args, Params) ->
    event(event_server, Host, JobName, Format, Args, Params).

-spec event(server(), host_name(), jobname(), nonempty_string(), list(), tuple()) -> ok.
event(EventServer, Host, JobName, Format, Args, Params) ->
    SArgs = [case lists:flatlength(io_lib:fwrite("~p", [X])) > 1000000 of
                 true -> trunc_io:fprint(X, 1000000);
                 false -> X
             end || X <- Args],
    RawMsg = disco:format(Format, SArgs),
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

-spec task_event(task(), term()) -> ok.
task_event(Task, Event) ->
    task_event(Task, Event, {}).

-spec task_event(task(), term(), tuple()) -> ok.
task_event(Task, Event, Params) ->
    task_event(Task, Event, Params, "master").

-spec task_event(task(), term(), tuple(), host_name()) -> ok.
task_event(Task, Event, Params, Host) ->
    task_event(Task, Event, Params, Host, event_server).

-spec task_event(task(), term(), tuple(), host_name(), server()) -> ok.
task_event(Task, {Type, Message}, Params, Host, EventServer) ->
    event(EventServer,
          Host,
          Task#task.jobname,
          "~s: [~s:~B] " ++ task_format(Message),
          [Type, Task#task.mode, Task#task.taskid, Message],
          Params);
task_event(Task, Message, Params, Host, EventServer) ->
    task_event(Task, {<<"SYS">>, Message}, Params, Host, EventServer).

task_format(Msg) when is_atom(Msg) or is_binary(Msg) or is_list(Msg) ->
    "~s";
task_format(_Msg) ->
    "~w".

% callback stubs
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

delete_jobdir(JobName) ->
    Safe = string:chr(JobName, $.) + string:chr(JobName, $/),
    if Safe =:= 0 ->
            _ = os:cmd("rm -Rf " ++ disco:jobhome(JobName)),
            ok;
       true ->
            ok
    end.

job_event_handler(JobName, JobCoordinator) ->
    {ok, _JobHome} = disco:make_dir(disco:jobhome(JobName)),
    {ok, File} = file:open(event_log(JobName), [append, raw]),
    gen_server:call(event_server, {job_initialized, JobName, self()}),
    gen_server:reply(JobCoordinator, {ok, JobName}),
    {ok, _} = timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
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
            {ok, _} = timer:send_after(?EVENT_BUFFER_TIMEOUT, flush),
            job_event_handler_do(File, [], 0);
        done ->
            flush_buffer(File, Buf),
            file:close(File);
        E ->
            lager:warning("Unknown job_event msg ~p", [E]),
            file:close(File)
    end.

flush_buffer(_, []) -> ok;
flush_buffer(File, Buf) ->
    ok = file:write(File, lists:reverse(Buf)).
