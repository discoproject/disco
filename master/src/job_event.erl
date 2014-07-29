-module(job_event).
-behaviour(gen_server).

-export([update/2, get_status/1, get_start/1, get_info/1, get_results/1,
        get_stage_results/2, add_event/4, done/1, purge/1, get_msgs/2,
        pending_event/3]).

-export([start_link/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([task_event/3, task_event/4, task_event/5]).
-export([event/5]).

-include("gs_util.hrl").
-include("event.hrl").

-define(EVENT_PAGE_SIZE, 100).
-define(EVENT_BUFFER_SIZE, 1000).
-define(EVENT_BUFFER_TIMEOUT, 2000).

-type stage_dict() :: disco_dict(stage_name(), non_neg_integer()).
-type stage_result_dict() :: disco_dict(stage_name(), [[url()]]).
-type task_event_info() :: {jobname(), stage_name(), task_id()}.
-type task_msg() :: {binary(), term()} | string().

-export_type([task_event_info/0, task_msg/0]).

-record(state, {job_coord :: pid(),
                timeout = infinity   :: hibernate | infinity,
                nmsgs                :: non_neg_integer(),
                msg_list_len         :: non_neg_integer(),
                msgs                 :: [binary()],
                job_name             :: jobname(),
                event_file           :: none | file:fd(),
                dirty_events         :: [binary()],
                n_dirty              :: non_neg_integer(),
                start                :: erlang:timestamp(),
                job_data = none      :: none | jobinfo(),
                task_count_inc       :: disco_dict(stage_name(), boolean()),
                task_count           :: stage_dict(),
                task_pending         :: stage_dict(),
                task_ready           :: stage_dict(),
                task_failed          :: stage_dict(),
                stage_results        :: stage_result_dict(),
                job_results = none   :: none | [url() | [url()]]}).
-type state() :: #state{}.

-spec start_link(pid(), jobname(), [stage_name()]) -> pid().
start_link(JC, JobName, Stages) ->
    {ok, Server} = gen_server:start_link(?MODULE, {JC, JobName, Stages}, []),
    Server.

-spec update(pid(), event()) -> ok.
update(JEHandler, Event) ->
    gen_server:cast(JEHandler, {update, Event}).

-spec get_msgs(pid(), integer()) -> [binary()].
get_msgs(JEHandler, N) ->
    gen_server:call(JEHandler, {get_msgs, N}).

-spec add_event(pid(), host(), binary(), event()) -> ok.
add_event(JEHandler, Host, Msg, Event) ->
    gen_server:cast(JEHandler, {add_event, Host, Msg, Event}).

-spec pending_event(pid(), stage_name(), add|remove) -> ok.
pending_event(JEHandler, Stage, add) ->
    update(JEHandler, {task_pending, Stage});
pending_event(JEHandler, Stage, remove) ->
    update(JEHandler, {task_un_pending, Stage}),
    update(JEHandler, {task_start, Stage}).

-spec get_status(pid()) -> job_status().
get_status(JEHandler) ->
    gen_server:call(JEHandler, get_status).

-spec get_start(pid()) -> erlang:timestamp().
get_start(JEHandler) ->
    gen_server:call(JEHandler, get_start).

-spec get_info(pid()) -> job_eventinfo().
get_info(JEHandler) ->
    gen_server:call(JEHandler, get_info).

-spec get_results(pid()) -> job_status().
get_results(JEHandler) ->
    gen_server:call(JEHandler, get_results).

-spec done(pid()) -> ok.
done(JEHandler) ->
    gen_server:cast(JEHandler, done).

-spec purge(pid()) -> ok.
purge(JEHandler) ->
    gen_server:cast(JEHandler, purge).

-spec get_stage_results(pid(), stage_name()) ->  not_ready | {ok, [[url()]]}.
get_stage_results(JEHandler, StageName) ->
    gen_server:call(JEHandler, {get_stage_results, StageName}).


-spec task_event(pid(), task_event_info(), task_msg()) -> ok.
task_event(JEHandler, Task, Msg) ->
    task_event(JEHandler, Task, Msg, none).

-spec task_event(pid(), task_event_info(), task_msg(), event()) -> ok.
task_event(JEHandler, Task, Msg, Event) ->
    task_event(JEHandler, Task, Msg, Event, "master").

-spec task_event(pid(), task_event_info(), task_msg(), event(), host()) -> ok.
task_event(JEHandler, {_JN, Stage, TaskId}, {Type, Message}, Event, Host) ->
    event(JEHandler, Host,
          "~s: [~s:~B] " ++ msg_format(Message),
          [Type, Stage, TaskId, Message],
          Event);
task_event(JEHandler, Task, Message, Event, Host) ->
    task_event(JEHandler, Task, {<<"SYS">>, Message}, Event, Host).

-spec msg_format(term()) -> nonempty_string().
msg_format(Msg) when is_atom(Msg) or is_binary(Msg) or is_list(Msg) ->
    "~s";
msg_format(_Msg) ->
    "~w".

-spec event(pid(), host(), nonempty_string(), list(), event()) -> ok.
event(JEHandler, Host, MsgFormat, Args, Event) ->
    RawMsg = disco:format(MsgFormat, Args),
    Json = try mochijson2:encode(list_to_binary(RawMsg))
           catch _:_ ->
                   Hex = ["WARNING: Binary message data: ",
                          [io_lib:format("\\x~2.16.0b",[N]) || N <- RawMsg]],
                   mochijson2:encode(list_to_binary(Hex))
           end,
    Msg = list_to_binary(Json),
    gen_server:cast(JEHandler, {add_event, Host, Msg, Event}).


-spec init({pid(), jobname(), [stage_name()]}) -> gs_init().
init({JC, JobName, Stages}) ->
    InitCounts = dict:from_list([{S, 0} || S <- Stages]),
    StageResults = dict:new(),
    {ok, _JobHome} = disco:make_dir(disco:jobhome(JobName)),
    {ok, File} = file:open(event_log(JobName), [append, raw]),
    add_event(self(), "master", <<"\"New job initialized!\"">>, none),
    erlang:send_after(?EVENT_BUFFER_TIMEOUT, self(), flush),
    S = #state{job_coord = JC,
             job_name = JobName,
             event_file = File,
             nmsgs = 0,
             msg_list_len = 0,
             msgs = [],
             start     = now(),
             dirty_events = [],
             n_dirty = 0,
             task_count_inc= dict:from_list([{S, true} || S <- Stages]),
             task_count    = InitCounts,
             task_pending  = InitCounts,
             task_ready    = InitCounts,
             task_failed   = InitCounts,
             stage_results = StageResults},
    {ok, S}.

-spec handle_call(term(), from(), state()) -> gs_reply(term()) | gs_noreply().
handle_call(get_status, _From, S) ->
    {reply, do_get_status(S), S, S#state.timeout};

handle_call(get_start, _From, S) ->
    {reply, do_get_start(S), S, S#state.timeout};

handle_call(get_info, _From, S) ->
    {reply, do_get_info(S), S, S#state.timeout};

handle_call(get_results, _From, S) ->
    {reply, do_get_results(S), S, S#state.timeout};

handle_call({get_stage_results, StageName}, _From, S) ->
    {reply, do_get_stage_results(S, StageName), S, S#state.timeout};

handle_call({get_msgs, N}, _From, S) ->
    {reply, do_get_msgs(N, S), S, S#state.timeout}.

-spec handle_cast(update, state()) -> gs_noreply();
                 ({add_event, host(), binary(), event()}, state()) -> gs_noreply().
handle_cast({add_event, Host, Msg, Event}, S) ->
    {noreply, do_add_event(Host, Msg, Event, S)};

handle_cast({update, Event}, S) ->
    {noreply, do_update(S, Event)};

handle_cast(done, S) ->
    {noreply, do_done(S), hibernate};

handle_cast(purge, S) ->
    {stop, normal, do_done(S)}.

-spec handle_info(flush, state()) -> gs_noreply().
handle_info(flush, #state{event_file = none, dirty_events = []} = S) ->
    {noreply, S};
handle_info(flush, #state{event_file = none, job_name = JN} = S) ->
    lager:info("job ~p has events not written to disk.", [JN]),
    {noreply, S};
handle_info(flush, #state{event_file = File, dirty_events = DirtyBuf} = S) ->
    erlang:send_after(?EVENT_BUFFER_TIMEOUT, self(), flush),
    do_flush(File, DirtyBuf),
    {noreply, S#state{dirty_events = [], n_dirty = 0}}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _S) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_Old, S, _E) -> {ok, S}.

-spec do_update(state(), event()) -> state().
do_update(JE, none) -> JE;
do_update(JE, {job_data, JobData}) ->
    JE#state{job_data = JobData};
do_update(#state{task_count = TaskCount, task_count_inc = Inc} = JE,
               {stage_start, Stage, Tasks}) ->
    JE#state{task_count = dict:store(Stage, Tasks, TaskCount),
               task_count_inc = dict:store(Stage, false, Inc)};
do_update(#state{task_count = TaskCount, task_count_inc = Inc} = JE, {task_start, Stage}) ->
    case dict:fetch(Stage, Inc) of
        true  -> JE#state{task_count = dict:update_counter(Stage, 1, TaskCount)};
        false -> JE
    end;
do_update(#state{task_pending = TaskPending} = JE, {task_pending, Stage}) ->
    JE#state{task_pending= dict:update_counter(Stage, 1, TaskPending)};
do_update(#state{task_pending = TaskPending} = JE, {task_un_pending, Stage}) ->
    JE#state{task_pending= dict:update_counter(Stage, -1, TaskPending)};
do_update(#state{task_ready = TaskReady} = JE, {task_ready, Stage}) ->
    JE#state{task_ready = dict:update_counter(Stage, 1, TaskReady)};
do_update(#state{task_failed = TaskFailed} = JE, {task_failed, Stage}) ->
    JE#state{task_failed = dict:update_counter(Stage, 1, TaskFailed)};
do_update(#state{stage_results = Results} = JE, {stage_ready, S, R}) ->
    JE#state{stage_results = dict:store(S, R, Results)};
do_update(JE, {ready, Results}) ->
    JE#state{job_results = Results}.

do_add_event(Host0, Msg, Event,
             #state{
                msgs = MsgLst0, nmsgs = NMsg, msg_list_len = LstLen0
            } = S) ->
    Time = disco_util:format_timestamp(now()),
    Host = list_to_binary(Host0),
    Line = <<"[\"",
        Time/binary, "\",\"",
        Host/binary, "\",",
        Msg/binary, "]", 10>>,
    if
        LstLen0 + 1 > ?EVENT_PAGE_SIZE * 2 ->
            MsgLst = lists:sublist([Line|MsgLst0], ?EVENT_PAGE_SIZE),
            LstLen = ?EVENT_PAGE_SIZE;
        true ->
            MsgLst = [Line|MsgLst0],
            LstLen = LstLen0 + 1
    end,
    S1 = do_update(S, Event),
    S2 = dirty_append(Line, S1),
    S2#state{nmsgs = NMsg + 1, msg_list_len = LstLen, msgs = MsgLst}.


dirty_append(Line, #state{event_file = File, dirty_events = DirtyEvents,
                          n_dirty = NDirty} = S) when NDirty > ?EVENT_BUFFER_SIZE ->
    do_flush(File, [Line|DirtyEvents]),
    S#state{n_dirty = 0, dirty_events = []};
dirty_append(Line, #state{dirty_events = DirtyEvents, n_dirty = NDirty} = S) ->
    S#state{n_dirty = NDirty + 1, dirty_events = [Line|DirtyEvents]}.

-spec do_get_status(state()) -> job_status().
do_get_status(#state{job_coord = Pid, job_results = none}) ->
    case is_process_alive(Pid) of
        true  -> active;
        false -> dead
    end;
do_get_status(_S) ->
    ready.

-spec do_get_start(state()) -> erlang:timestamp().
do_get_start(#state{start = Start}) ->
    Start.

-spec do_get_info(state()) -> job_eventinfo().
do_get_info(#state{start = Start, job_data = JobNfo, job_results = Results,
                   task_count = Count, task_pending = Pending,
                   task_ready = Ready, task_failed = Failed} = S) ->
    {Start,
     do_get_status(S), JobNfo,
     case Results of none -> []; _ -> Results end,
     Count,
     Pending,
     Ready,
     Failed}.

-spec do_get_results(state())
                    -> invalid_job | {job_status(), pid()}
                           | {ready, pid(), [url() | [url()]]}.
do_get_results(S) ->
    case S of
        #state{job_coord = Pid, job_results = Res} when Res =/= none ->
            {ready, Pid, Res};
        #state{job_coord = Pid} ->
            {do_get_status(S), Pid}
    end.

-spec do_get_msgs(non_neg_integer(), state()) -> [binary()].
do_get_msgs(N, #state{msgs = MsgLst}) ->
    event_server:json_list(lists:sublist(MsgLst, N)).

-spec do_get_stage_results(state(), stage_name()) -> not_ready | {ok, [[url()]]}.
do_get_stage_results(#state{stage_results = PR}, StageName) ->
    case dict:find(StageName, PR) of
        error -> not_ready;
        {ok, _Res} = Ret -> Ret
    end.

-spec do_done(state()) -> state().
do_done(#state{event_file = none} = S) ->
    S#state{dirty_events = [], n_dirty = 0, event_file = none};
do_done(#state{event_file = File, dirty_events = DirtyBuf} = S) ->
    do_flush(File, DirtyBuf),
    ok = file:close(File),
    S#state{dirty_events = [], n_dirty = 0, event_file = none,
            timeout = hibernate}.

event_log(JobName) ->
    filename:join(disco:jobhome(JobName), "events").

do_flush(_, []) -> ok;
do_flush(none, _) -> ok; % if the file is closed, just drop the events.
do_flush(File, Buf) ->
    ok = file:write(File, lists:reverse(Buf)).
