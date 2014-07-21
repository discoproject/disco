-module(job_event).
-behaviour(gen_server).

-export([update/2, get_status/1, get_start/1, get_info/1, get_results/1,
        get_stage_results/2]).

-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("gs_util.hrl").
-include("event.hrl").

-type stage_dict() :: disco_dict(stage_name(), non_neg_integer()).
-type stage_result_dict() :: disco_dict(stage_name(), [[url()]]).

-record(state, {job_coord :: pid(),
                  start     :: erlang:timestamp(),
                  job_data    = none :: none | jobinfo(),
                  task_count_inc     :: disco_dict(stage_name(), boolean()),
                  task_count         :: stage_dict(),
                  task_pending       :: stage_dict(),
                  task_ready         :: stage_dict(),
                  task_failed        :: stage_dict(),
                  stage_results      :: stage_result_dict(),
                  job_results = none :: none | [url() | [url()]]}).
-type state() :: #state{}.

-spec start_link(pid(), [stage_name()]) -> pid().
start_link(JC, Stages) ->
    {ok, Server} = gen_server:start_link(?MODULE, {JC, Stages}, []),
    Server.

-spec init({pid(), [stage_name()]}) -> gs_init().
init({JC, Stages}) ->
    InitCounts = dict:from_list([{S, 0} || S <- Stages]),
    StageResults = dict:new(),
    S = #state{job_coord = JC,
             start     = now(),
             task_count_inc= dict:from_list([{S, true} || S <- Stages]),
             task_count    = InitCounts,
             task_pending  = InitCounts,
             task_ready    = InitCounts,
             task_failed   = InitCounts,
             stage_results = StageResults},
    {ok, S}.

-spec handle_call(term(), from(), state()) -> gs_reply(term()) | gs_noreply().
handle_call(get_status, _From, S) ->
    {reply, do_get_status(S), S};

handle_call(get_start, _From, S) ->
    {reply, do_get_start(S), S};

handle_call(get_info, _From, S) ->
    {reply, do_get_info(S), S};

handle_call(get_results, _From, S) ->
    {reply, do_get_results(S), S};

handle_call({get_stage_results, StageName}, _From, S) ->
    {reply, do_get_stage_results(S, StageName), S}.

-spec handle_cast(term(), state()) -> gs_noreply().
handle_cast({update, Event}, S) ->
    {noreply, do_update(S, Event)}.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(_Msg, S) ->
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _S) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_Old, S, _E) -> {ok, S}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public API

-spec update(pid(), event()) -> ok.
update(JEHandler, Event) ->
    gen_server:cast(JEHandler, {update, Event}).

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

-spec get_stage_results(pid(), stage_name()) ->  not_ready | {ok, [[url()]]}.
get_stage_results(JEHandler, StageName) ->
    gen_server:call(JEHandler, {get_stage_results, StageName}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Private API

-spec do_update(state(), event()) -> state().
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

-spec do_get_stage_results(state(), stage_name()) -> not_ready | {ok, [[url()]]}.
do_get_stage_results(#state{stage_results = PR}, StageName) ->
    case dict:find(StageName, PR) of
        error -> not_ready;
        {ok, _Res} = Ret -> Ret
    end.
