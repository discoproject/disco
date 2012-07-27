-module(event_server).
-behaviour(gen_server).

% Cluster state.
-export([update_nodes/1]).
% Job notification.
-export([new_job/3, end_job/1, clean_job/1]).
% Retrieval.
-export([get_jobs/0, get_jobs/1, get_jobinfo/1, get_job_msgs/3,
         get_map_results/1, get_results/1]).
% Event logging.
-export([event/4, event/5, event/6,
         task_event/2, task_event/3, task_event/4, task_event/5,
         job_done_event/2]).
% Server.
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-define(EVENT_PAGE_SIZE, 100).
-define(EVENT_BUFFER_SIZE, 1000).
-define(EVENT_BUFFER_TIMEOUT, 2000).

-type event() :: {job_data, jobinfo()}
               | {task_ready, stage_name()}
               | {task_failed, stage_name()}
               | {stage_ready, stage_name(), [[url()]]}
               | {ready, [[url()]]}.

-type task_info() :: {jobname(), stage_name(), task_id()}.

-type task_msg() :: {binary(), term()} | string().

-type job_status() :: active | dead | ready.

-type joblist_entry() :: {JobName :: jobname(),
                          job_status(),
                          StartTime :: erlang:timestamp()}.

-type job_eventinfo() :: {StartTime :: erlang:timestamp(),
                          Status  :: job_status(),
                          JobInfo :: none | jobinfo(),
                          Results :: [[url()]],
                          Ready   :: dict(),
                          Failed  :: dict()}.

-export_type([event/0, task_info/0, task_msg/0, job_eventinfo/0]).

% Cluster configuration.

-spec update_nodes([host()]) -> ok.
update_nodes(Hosts) ->
    gen_server:cast(?MODULE, {update_nodes, Hosts}).

% Job notification.

-spec new_job(jobname(), pid(), [stage_name()]) -> {ok, jobname(), [host()]}.
new_job(Prefix, JobCoord, Stages) ->
    gen_server:call(?MODULE, {new_job, Prefix, JobCoord, Stages}, 10000).

-spec end_job(jobname()) -> ok.
end_job(JobName) ->
    gen_server:cast(?MODULE, {job_done, JobName}).

-spec clean_job(jobname()) -> ok.
clean_job(JobName) ->
    gen_server:cast(?MODULE, {clean_job, JobName}).

% Retrieval.

-spec get_jobs() -> {ok, [joblist_entry()]}.
get_jobs() ->
    gen_server:call(?MODULE, get_jobs).

-spec get_jobs(Master :: node()) -> {ok, [joblist_entry()]}.
get_jobs(Master) ->
    gen_server:call({?MODULE, Master}, get_jobs).

-spec get_jobinfo(jobname()) -> invalid_job | {ok, job_eventinfo()}.
get_jobinfo(JobName) ->
    gen_server:call(?MODULE, {get_jobinfo, JobName}).

-spec get_job_msgs(jobname(), string(), integer()) -> {ok, [binary()]}.
get_job_msgs(JobName, Q, N) ->
    gen_server:call(?MODULE, {get_job_msgs, JobName, string:to_lower(Q), N}).

-spec get_map_results(jobname()) -> invalid_job | not_ready
                                        | {ok, [[url()]]}.
get_map_results(JobName) ->
    gen_server:call(?MODULE, {get_map_results, JobName}).

-spec get_results(jobname()) -> invalid_job | {ready, pid(), [[url()]]}
                                    | {job_status(), pid()}.
get_results(JobName) ->
    gen_server:call(?MODULE, {get_results, JobName}).

% Event logging.

-spec event(jobname(), nonempty_string(), list(), none | event()) -> ok.
event(JobName, MsgFormat, Args, Event) ->
    event("master", JobName, MsgFormat, Args, Event).

-spec event(host(), jobname(), nonempty_string(), list(), none | event()) -> ok.
event(Host, JobName, MsgFormat, Args, Event) ->
    event(?MODULE, Host, JobName, MsgFormat, Args, Event).

-spec event(server(), host(), jobname(), nonempty_string(),
            list(), none | event()) -> ok.
event(EventServer, Host, JobName, MsgFormat, Args, Event) ->
    RawMsg = disco:format(MsgFormat, Args),
    Json = try mochijson2:encode(list_to_binary(RawMsg))
           catch _:_ ->
                   Hex = ["WARNING: Binary message data: ",
                          [io_lib:format("\\x~2.16.0b",[N]) || N <- RawMsg]],
                   mochijson2:encode(list_to_binary(Hex))
           end,
    Msg = list_to_binary(Json),
    gen_server:cast(EventServer, {add_job_event, Host, JobName, Msg, Event}).

-spec job_done_event(jobname(), [[url()]]) -> ok.
job_done_event(JobName, Results) ->
    gen_server:cast(?MODULE, {job_done_event, JobName, Results}).

-spec task_event(task_info(), task_msg()) -> ok.
task_event(Task, Msg) ->
    task_event(Task, Msg, none).

-spec task_event(task_info(), task_msg(), none | event()) -> ok.
task_event(Task, Msg, Event) ->
    task_event(Task, Msg, Event, "master").

-spec task_event(task_info(), task_msg(), none | event(), host()) -> ok.
task_event(Task, Msg, Event, Host) ->
    task_event(Task, Msg, Event, Host, ?MODULE).

-spec task_event(task_info(), task_msg(), none | event(), host(), server()) -> ok.
task_event({JN, Stage, TaskId}, {Type, Message}, Event, Host, EventServer) ->
    event(EventServer, Host, JN,
          "~s: [~s:~B] " ++ msg_format(Message),
          [Type, Stage, TaskId, Message],
          Event);
task_event(Task, Message, Event, Host, EventServer) ->
    task_event(Task, {<<"SYS">>, Message}, Event, Host, EventServer).

-spec msg_format(term()) -> nonempty_string().
msg_format(Msg) when is_atom(Msg) or is_binary(Msg) or is_list(Msg) ->
    "~s";
msg_format(_Msg) ->
    "~w".

% Server.

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("Event server starts"),
    case gen_server:start_link({local, ?MODULE}, ?MODULE, [], []) of
        {ok, Server} -> {ok, Server};
        {error, {already_started, Server}} -> {ok, Server}
    end.

% events dict: jobname -> job_ent()
% msgbuf dict: jobname -> { <nmsgs>, <list-length>, [<msg>] }
-record(state, {events = dict:new() :: dict(),
                msgbuf = dict:new() :: dict(),
                hosts  = []         :: [host()]}).
-type state() :: #state{}.

-spec init(_) -> gs_init().
init(_Args) ->
    _ = ets:new(event_files, [named_table]),
    {ok, #state{}}.

-spec handle_call(term(), from(), state()) -> gs_reply(term()) | gs_noreply().

handle_call({new_job, JobPrefix, Pid, Stages}, From,
            #state{events = Events0, msgbuf = MsgBuf0, hosts = Hosts} = S) ->
    case unique_key(JobPrefix, Events0) of
        invalid_prefix ->
            {reply, {error, invalid_prefix}, S};
        {ok, JobName} ->
            Events = dict:store(JobName, new_job_ent(Pid, Stages), Events0),
            MsgBuf = dict:store(JobName, {0, 0, []}, MsgBuf0),
            spawn(fun() -> job_event_handler(JobName, Hosts, From) end),
            {noreply, S#state{events = Events, msgbuf = MsgBuf}}
    end;
handle_call(get_jobs, _F, #state{events = Events} = S) ->
    {reply, do_get_jobs(Events), S};
handle_call({get_job_msgs, JobName, Query, N}, _F, #state{msgbuf = MB} = S) ->
    {reply, do_get_job_msgs(JobName, Query, N, MB), S};
handle_call({get_results, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_results(JobName, Events), S};
handle_call({get_map_results, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_map_results(JobName, Events), S};
handle_call({job_initialized, JobName, JobEventHandler}, _F, S) ->
    ets:insert(event_files, {JobName, JobEventHandler}),
    S1 = add_event("master", JobName, <<"\"New job initialized!\"">>, none, S),
    {reply, ok, S1};
handle_call({get_jobinfo, JobName}, _F, #state{events = Events} = S) ->
    {reply, do_get_jobinfo(JobName, Events), S}.

-spec handle_cast(term(), state()) -> gs_noreply().

handle_cast({update_nodes, Hosts}, S) ->
    {noreply, S#state{hosts = Hosts}};
handle_cast({add_job_event, Host, JobName, Msg, Event}, S) ->
    {noreply, do_add_job_event(Host, JobName, Msg, Event, S)};
handle_cast({job_done_event, JobName, Results}, S) ->
    {noreply, do_job_done_event(JobName, Results, S)};
% XXX: Some aux process could go through the jobs periodically and
% check that the job coord is still alive - if not, call job_done for
% the zombie job.
handle_cast({job_done, JobName}, S) ->
    {noreply, do_job_done(JobName, S)};
handle_cast({clean_job, JobName}, S) ->
    #state{events = Events} = S1 = do_job_done(JobName, S),
    delete_jobdir(JobName),
    {noreply, S1#state{events = dict:erase(JobName, Events)}}.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(Msg, State) ->
    lager:warning("Unknown message received: ~p", [Msg]),
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

% State management.

-record(job_ent, {job_coord :: pid(),
                  start     :: erlang:timestamp(),
                  job_data    = none :: none | jobinfo(),
                  task_ready         :: dict(), % stage_name() -> count
                  task_failed        :: dict(), % stage_name() -> count
                  stage_results      :: dict(), % stage_name() -> results
                  job_results = none :: none | [[url()]]}).
-type job_ent() :: #job_ent{}.

-spec new_job_ent(pid(), [stage_name()]) -> job_ent().
new_job_ent(JobCoord, Stages) ->
    Counts = [{S, 0} || S <- Stages],
    TaskReady  = dict:from_list(Counts),
    TaskFailed = dict:from_list(Counts),
    StageResults = dict:new(),
    #job_ent{job_coord = JobCoord,
             start     = now(),
             task_ready    = TaskReady,
             task_failed   = TaskFailed,
             stage_results = StageResults}.

-spec update_job_ent(job_ent(), event()) -> job_ent().
update_job_ent(JE, {job_data, JobData}) ->
    JE#job_ent{job_data = JobData};
update_job_ent(#job_ent{task_ready = TaskReady} = JE, {task_ready, Stage}) ->
    JE#job_ent{task_ready = dict:update_counter(Stage, 1, TaskReady)};
update_job_ent(#job_ent{task_failed = TaskFailed} = JE, {task_failed, Stage}) ->
    JE#job_ent{task_failed = dict:update_counter(Stage, 1, TaskFailed)};
update_job_ent(#job_ent{stage_results = Results} = JE, {stage_ready, S, R}) ->
    JE#job_ent{stage_results = dict:store(S, R, Results)};
update_job_ent(JE, {ready, Results}) ->
    JE#job_ent{job_results = Results}.

-spec job_status(job_ent()) -> job_status().
job_status(#job_ent{job_coord = Pid, job_results = none}) ->
    case is_process_alive(Pid) of
        true  -> active;
        false -> dead
    end;
job_status(_JE) ->
    ready.

% Server implemention.

-spec do_get_jobs(dict()) -> {ok, [joblist_entry()]}.
do_get_jobs(Events) ->
    Jobs = dict:fold(fun (Name, #job_ent{start = Start} = JobEnt, Acc) ->
                             [{Name, job_status(JobEnt), Start}|Acc]
                     end, [], Events),
    {ok, Jobs}.

-spec do_get_job_msgs(jobname(), string(), integer(), dict()) -> {ok, [binary()]}.
do_get_job_msgs(JobName, Query, N0, MsgBuf) ->
    N = if
            N0 > 1000 -> 1000;
            true -> N0
        end,
    case dict:find(JobName, MsgBuf) of
        _ when Query =/= "" ->
            {ok, json_list(grep_log(JobName, Query, N))};
        {ok, {_NMsg, _ListLength, MsgLst}} ->
            {ok, json_list(lists:sublist(MsgLst, N))};
        error ->
            {ok, json_list(tail_log(JobName, N))}
    end.

-spec do_get_results(jobname(), dict())
                    -> invalid_job | {job_status(), pid()}
                           | {ready, pid(), [[url()]]}.
do_get_results(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, #job_ent{job_coord = Pid, job_results = Res}} when Res =/= none ->
            {ready, Pid, Res};
        {ok, #job_ent{job_coord = Pid} = JE} ->
            {job_status(JE), Pid}
    end.

-spec do_get_map_results(jobname(), dict())
                        -> invalid_job | not_ready
                               | {ok, [[url()]]}.
do_get_map_results(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, #job_ent{stage_results = PR}} ->
            case dict:find(?MAP, PR) of
                error -> not_ready;
                {ok, _Res} = Ret -> Ret
            end
    end.

-spec do_get_jobinfo(jobname(), dict()) -> invalid_job | {ok, job_eventinfo()}.
do_get_jobinfo(JobName, Events) ->
    case dict:find(JobName, Events) of
        error ->
            invalid_job;
        {ok, #job_ent{start = Start, job_data = JobNfo, job_results = Results,
                      task_ready = Ready, task_failed = Failed} = JE} ->
            {ok, {Start, job_status(JE), JobNfo,
                  case Results of none -> []; _ -> Results end,
                  Ready, Failed}}
    end.

-spec do_add_job_event(host(), jobname(), binary(), event(), state()) -> state().
do_add_job_event(Host, JobName, Msg, Event, #state{msgbuf = MsgBuf} = S) ->
    case dict:is_key(JobName, MsgBuf) of
        true -> add_event(Host, JobName, Msg, Event, S);
        false -> S
    end.

add_event(Host0, JobName, Msg, Event,
          #state{events = Events, msgbuf = MsgBuf} = S) ->
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
        Event =:= none ->
            S#state{events = Events, msgbuf = MsgBufN};
        true ->
            {ok, JE} = dict:find(JobName, Events),
            EventsN = dict:store(JobName, update_job_ent(JE, Event), Events),
            S#state{events = EventsN, msgbuf = MsgBufN}
    end.

-spec do_job_done(jobname(), state()) -> state().
do_job_done(JobName, #state{msgbuf = MsgBuf} = S) ->
    case ets:lookup(event_files, JobName) of
        [] ->
            ok;
        [{_, EventProc}] ->
            EventProc ! done,
            ets:delete(event_files, JobName)
    end,
    case dict:find(JobName, MsgBuf) of
        error -> S;
        {ok, _} -> S#state{msgbuf = dict:erase(JobName, MsgBuf)}
    end.

-spec do_job_done_event(jobname(), [[url()]], state()) -> state().
do_job_done_event(JobName, Results, #state{events = Events} = S) ->
    case dict:find(JobName, Events) of
        {ok, #job_ent{start = Start}} ->
            Msg = disco:format("READY: Job done in ~s",
                               [disco:format_time_since(Start)]),
            Event = {ready, Results},
            add_event("master", JobName, list_to_binary(Msg), Event, S);
        error ->
            S
    end.

% Flush events from memory to file using a per-job process.

job_event_handler(JobName, Hosts, JobCoordinator) ->
    {ok, _JobHome} = disco:make_dir(disco:jobhome(JobName)),
    {ok, File} = file:open(event_log(JobName), [append, raw]),
    gen_server:call(?MODULE, {job_initialized, JobName, self()}),
    gen_server:reply(JobCoordinator, {ok, JobName, Hosts}),
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

% Misc utilities

-spec json_list([binary()]) -> [binary()].
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
            false -> {ok, Key};
            true -> unique_key(Prefix, Dict)
        end
    end.

% Utilities to process job event file.

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

delete_jobdir(JobName) ->
    Safe = string:chr(JobName, $.) + string:chr(JobName, $/),
    if Safe =:= 0 ->
            _ = os:cmd("rm -Rf " ++ disco:jobhome(JobName)),
            ok;
       true ->
            ok
    end.
