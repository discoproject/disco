-module(job_coordinator).
-behaviour(gen_server).

-export([new/1, task_done/2, update_nodes/2, task_started/3, kill_job/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("config.hrl").
-include("pipeline.hrl").
-include("job_coordinator.hrl").

-define(DISCO_SERVER_TIMEOUT, 30000).

% In theory we could keep the HTTP connection pending until the job
% finishes but in practice long-living HTTP connections are a bad
% idea.  Thus, the HTTP request spawns a new process, job_coordinator,
% that takes care of coordinating the whole map-reduce show, including
% fault-tolerance. The HTTP request returns immediately. It may poll
% the job status e.g. by using handle_ctrl's get_results.
-spec new(binary()) -> {ok, jobname()}.
new(JobPack) ->
    Self = self(),
    process_flag(trap_exit, true),
    Pid = spawn_link(
            fun() ->
                    case jobpack:valid(JobPack) of
                        ok -> ok;
                        {error, E} -> exit(E)
                    end,
                    case start_job(Self, JobPack) of
                        ok -> ok;
                        {error, Err} -> exit(Err)
                    end
            end),
    receive
        {job_started, JobName} ->
            {ok, JobName};
        {'EXIT', _From, Reason} ->
            exit(Pid, kill),
            throw(Reason)
    after 60000 ->
            exit(Pid, kill),
            throw("timed out after 60s (master busy?)")
    end.

-spec start_job(pid(), binary()) -> ok | {error, term()}.
start_job(Starter, JobPack) ->
    case gen_server:start(?MODULE, {Starter, JobPack}, []) of
        {ok, _Pid} -> ok;
        {error, _E} = E -> E
    end.


-type task_done_msg() :: {error | fatal, term()}
                       | {input_error, {input_id(), [host()]}}
                       | {done, {none | binary(), [binary()]}}.
-type task_done_result() :: {error | fatal, term()}
                          | {input_error, {input_id(), [host()]}}
                          | {done, [task_output()]}.
-spec task_done(pid(), {task_done_msg(), task_id(), host()}) -> ok.
task_done(JobCoord, {TaskResults, TaskId, Host}) ->
    R = case TaskResults of
            {done, _R}  = D -> D;
            {error, _E} = E -> E;
            {fatal, _E} = E -> E;
            {input_error, {_IId, _UH}} = E -> E
        end,
    gen_server:cast(JobCoord, {task_done, TaskId, Host, R}).

-spec update_nodes(pid(), [host()]) -> ok.
update_nodes(JobCoord, Hosts) ->
    gen_server:cast(JobCoord, {update_nodes, Hosts}).

%% ===================================================================
%% internal API
-type submit_mode() :: first_run | re_run.
-spec submit_tasks(pid(), submit_mode(), [task_id()]) -> ok.
submit_tasks(JobCoord, Mode, Tasks) ->
    gen_server:cast(JobCoord, {submit_tasks, Mode, Tasks}).

-spec kill_job(term(), pid()) -> ok.
kill_job(Reason, JC) ->
    gen_server:cast(JC, {kill_job, Reason}).

-spec kill_job(term()) -> ok.
kill_job(Reason) ->
    kill_job(Reason, self()).

-spec use_inputs(pid(), {ok, [task_output()]} | {error, term()}) -> ok.
use_inputs(Coord, Inputs) ->
    gen_server:cast(Coord, {inputs, Inputs}).

-spec stage_done(stage_name()) -> ok.
stage_done(Stage) ->
    gen_server:cast(self(), {stage_done, Stage}).

-spec task_started(pid(), task_id(), pid()) -> ok.
task_started(Coord, TaskId, Worker) ->
    gen_server:cast(Coord, {task_started, TaskId, Worker}).

%% ===================================================================
%% gen_server callbacks

% Internal state of the job coordinator.
-record(state, {jobinfo          :: jobinfo(),
                pipeline         :: pipeline(),
                schedule         :: task_schedule(),
                next_taskid = 0  :: task_id(),
                next_runid  = 0  :: task_run_id(),
                input_pid = none :: none | pid(),
                jehandler        :: pid(),
                hosts      = gb_sets:empty()  :: disco_gbset(host()),
                tasks      = gb_trees:empty() :: disco_gbtree(task_id(), task_info()),
                data_map   = gb_trees:empty() :: disco_gbtree(input_id(), data_info()),
                pending    = gb_sets:empty()  :: disco_gbset({task_id(), submit_mode()}),
                stage_info = gb_trees:empty() :: disco_gbtree(stage_name(), stage_info())}).
-type state() :: #state{}.

-spec init({pid(), binary()}) -> gs_init() | {stop, term()}.
init({Starter, JobPack}) ->
    link(Starter),
    process_flag(trap_exit, true),
    try  {#jobinfo{jobname = Name} = Info, Hosts} = setup_job(JobPack, self()),
         Starter ! {job_started, Name},
         event_server:event(Name, "Created job ~p", [Name], {job_data, Info}),
         {ok, init_state(Info, Hosts)}
    catch
        {error, E} ->
            {stop, E};
        K:V ->
            lager:error("job submission failed: ~p", [erlang:get_stacktrace()]),
            {stop, disco:format("job init error: ~p:~p", [K, V])}
    end.

-spec handle_call(term(), from(), state()) -> gs_noreply().
handle_call(_M, _F, S) ->
    {noreply, S}.

-spec handle_cast({start, [task_output()]}, state()) -> gs_noreply();
                 ({update_nodes, [host()]}, state()) -> gs_noreply();
                 ({inputs, {ok, [task_output()]} | {error, term()}}, state()) ->
                         gs_noreply();
                 ({stage_done, stage_name()}, state()) -> gs_noreply();
                 ({submit_tasks, submit_mode(), [task_id()]}, state()) ->
                         gs_noreply();
                 ({task_done, task_id(), host(), [task_output()]}, state()) ->
                         gs_noreply();
                 (pipeline_done, state()) -> gs_noreply();
                 ({task_started, task_id(), pid()}, state()) -> gs_noreply();
                 ({kill_job, term()}, state()) -> gs_noreply().
handle_cast({start, Inputs}, S) ->
    {noreply, do_start(Inputs, S)};
handle_cast({update_nodes, Hosts}, S) ->
    {noreply, do_update_nodes(Hosts, S)};
handle_cast({inputs, {error, E}}, S) ->
    lager:warning("fetching inputs failed: ~p", [erlang:get_stacktrace()]),
    {stop, {"fetching inputs failed", E}, S#state{input_pid = none}};
handle_cast({inputs, {ok, Inputs}}, S) ->
    {noreply, do_use_inputs(Inputs, S)};
handle_cast({stage_done, Stage}, S) ->
    {noreply, do_stage_done(Stage, S)};
handle_cast({submit_tasks, Mode, Tasks}, S) ->
    {noreply, do_submit_tasks(Mode, Tasks, S)};
handle_cast({task_done, TaskId, Host, Results}, S) ->
    {noreply, do_task_done(TaskId, Host, Results, S)};
handle_cast(pipeline_done, #state{jobinfo = #jobinfo{jobname = JobName}} = S) ->
    event_server:end_job(JobName),
    {stop, normal, S};
handle_cast({task_started, TaskId, W}, S) ->
    {noreply, do_task_started(TaskId, W, S)};
handle_cast({kill_job, Reason}, S) ->
    do_kill_job(Reason, S),
    {stop, normal, S}.

-spec handle_info(term(), state()) -> gs_noreply() | gs_stop(tuple()).
handle_info({'EXIT', Pid, Reason}, #state{input_pid = Pid} = S) ->
    lager:warning("stopping due to input_pid (~p) failure: ~p (self ~p)",
                  [Pid, Reason, self()]),
    {stop, {"input_processor exited", Reason}, S#state{input_pid = none}};
handle_info({'EXIT', _Pid, normal}, S) ->
    {noreply, S};

handle_info({'EXIT', _Pid, kill_worker}, S) ->
    {stop, normal, S};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, S) ->
    lager:warning("Monitored process ~p goes down, stopping (self ~p).", [Pid, self()]),
    {stop, normal, S};

handle_info(M, S) ->
    lager:warning("Ignoring unexpected info event: ~p (self ~p)", [M, self()]),
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(normal, _S) ->
    ok;
terminate(Reason, #state{jobinfo = #jobinfo{jobname = JobName}}) ->
    lager:warning("job coordinator for ~s dies: ~p", [JobName, Reason]).

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.


%% ===================================================================
%% initialization

% This function performs all the failure-prone operations involved in
% initializing the job state.  It throws error exceptions on
% encountering problems, which are caught in init().
-spec setup_job(binary(), pid()) -> {jobinfo(), [host()]}.
setup_job(JobPack, JobCoord) ->
    {Prefix, JobInfo} = jobpack:jobinfo(JobPack),
    Stages = pipeline_utils:stages(JobInfo#jobinfo.pipeline),
    {ok, JobName, Hosts} = event_server:new_job(Prefix, JobCoord, Stages),
    JobFile = case jobpack:save(JobPack, disco:jobhome(JobName)) of
                  {ok, File}      -> File;
                  {error, _M} = T -> throw(T)
              end,
    erlang:monitor(process, whereis(disco_server)),
    case disco_server:new_job(JobName, JobCoord, 30000) of
        ok -> ok;
        {error, _E} = T1 -> throw(T1)
    end,
    {JobInfo#jobinfo{jobname = JobName, jobfile = JobFile}, Hosts}.

-spec init_state(jobinfo(), [host()]) -> state().
init_state(#jobinfo{jobname  = JobName,
                    schedule = Schedule,
                    inputs   = Inputs,
                    pipeline = Pipeline} = JobInfo, Hosts) ->
    lager:info("initialized job ~p with pipeline ~p and inputs ~p",
               [JobName, Pipeline, Inputs]),
    gen_server:cast(self(), {start, Inputs}),
    #state{jobinfo    = JobInfo,
           pipeline   = Pipeline,
           schedule   = Schedule,
           jehandler  = event_server:get_job_event_handler(JobName),
           hosts      = gb_sets:from_list(Hosts)}.

preprocess_inputs(Coord, Inputs) ->
    % Fetch and parse any dir:// inputs to extract labels/sizes.
    RealInputs =
        lists:foldl(fun (_I, {error, _} = Err) ->
                            Err;
                        ({Id, I}, {ok, Acc}) ->
                            case worker_utils:annotate_input(I) of
                                {ok, ParsedI} -> {ok, [{Id, ParsedI} | Acc]};
                                {error, _} = Err -> Err
                            end
                    end, {ok, []}, Inputs),
    use_inputs(Coord, RealInputs).

%% ===================================================================
%% state access and update utils

-spec stage_outputs(stage_name(), state()) -> [{task_id(), [task_output()]}].
stage_outputs(Stage, #state{stage_info = SI, tasks = Tasks}) ->
    Done = (jc_utils:stage_info(Stage, SI))#stage_info.done,
    [{Id, jc_utils:task_outputs(Id, Tasks)} || Id <- Done].

%% ===================================================================
%% Callback implementations.
-spec do_start([task_output()], state()) -> state().
do_start(Inputs, S) ->
    % We need to convert any dir:// urls into pipeline form, which
    % involves retrieving and parsing the dir files.  We hence do this
    % in a separate process.
    Coord = self(),
    InputPid = spawn_link(fun() -> preprocess_inputs(Coord, Inputs) end),
    S#state{input_pid = InputPid}.

do_task_started(TaskId, W, #state{tasks = Tasks, data_map = DataMap} = S) ->
    TaskInfo = jc_utils:task_info(TaskId, Tasks),
    Tasks1 = jc_utils:update_task_info(TaskId, TaskInfo#task_info{worker=W}, Tasks),
    case TaskInfo#task_info.new_input of
        true ->
            #task_info{spec = TaskSpec} = TaskInfo,
            #task_spec{input = Input} = TaskSpec,
            Inputs = jc_utils:task_inputs(Input, DataMap),
            disco_worker:add_inputs(W, Inputs);
        false ->
            ok
    end,
    case TaskInfo#task_info.end_input of
        true ->
            disco_worker:terminate_inputs(W);
        false ->
            ok
    end,
    S#state{tasks = Tasks1}.

-spec do_use_inputs([task_output()], state()) -> state().
do_use_inputs(Inputs, #state{jobinfo = JobInfo} = S) ->
    % Create a dummy completed 'input' task.
    Tasks = gb_trees:from_orddict([{input,
                                    #task_info{spec = input,
                                               outputs = Inputs}}]),
    % Mark the 'input' stage as done, and send notification.
    InputStage = #stage_info{all = 1, done = [input]},
    SI = gb_trees:from_orddict([{?INPUT, InputStage}]),
    stage_done(?INPUT),
    S#state{jobinfo    = JobInfo#jobinfo{inputs = Inputs},
            input_pid  = none,
            stage_info = SI,
            tasks      = Tasks}.

-spec do_update_nodes([host()], state()) -> state().
do_update_nodes(Hosts, S) ->
    S#state{hosts = gb_sets:from_list(Hosts)}.

-spec do_task_done(task_id(), host(), task_done_result(), state()) -> state().
do_task_done(TaskId, Host, Result, #state{jobinfo = #jobinfo{jobname = JobName},
                                          tasks   = Tasks,
                                          data_map   = DataMap,
                                          jehandler  = JEHandler,
                                          stage_info = SI} = S) ->
    #task_info{spec = #task_spec{stage = Stage}}
        = TInfo = jc_utils:task_info(TaskId, Tasks),
    ETInfo = {JobName, Stage, TaskId},
    FEvent = {task_failed, Stage},
    case Result of
        {fatal, F} ->
            job_event:task_event(JEHandler, ETInfo, {<<"FATAL">>, F}, FEvent),
            kill_job(F),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            S#state{stage_info = SI1};
        {error, E} ->
            job_event:task_event(JEHandler, ETInfo, {<<"WARNING">>, E}, FEvent),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            retry_task(Host, E, TInfo, S#state{stage_info = SI1});
        {input_error, {{input, _}, _}} = E ->
            % If a pipeline input is inaccessible, we cannot re-run
            % the generating task since there isn't one, so we
            % fallback to retry-ing the task.
            job_event:task_event(JEHandler, ETInfo, {<<"WARNING">>, "Job input failed"},
                                    FEvent),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            retry_task(Host, E, TInfo, S#state{stage_info = SI1});
        {input_error, {InputId, InputHosts}} = E ->
            job_event:task_event(JEHandler, ETInfo, {<<"WARNING">>, "Input failed"},
                                    FEvent),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            % We assume here that the disco_worker has validated the
            % InputId.
            DInfo = jc_utils:input_info(InputId, DataMap),
            % Update failure counts.
            DInfo1 = jc_utils:update_input_failures(InputHosts, DInfo),
            DataMap1 = jc_utils:update_input_info(InputId, DInfo1, DataMap),
            S1 = S#state{data_map = DataMap1, stage_info = SI1},
            case jc_utils:find_usable_input_hosts(DInfo1) of
                [] ->
                    % We need to regenerate the input, since we have
                    % no usable locations for this input.
                    regenerate_input(TInfo, InputId, DInfo1, S1);
                [_|_] ->
                    % Retry the task with the still-usable locations.
                    retry_task(Host, E, TInfo, S1)
            end;
        {done, Outputs} ->
            job_event:task_event(JEHandler, ETInfo,
                                 disco:format("Received results from ~s", [Host]),
                                 {task_ready, Stage}),
            task_complete(TaskId, Host, Outputs, S)
    end.

-spec finish_pipeline(stage_name(), state()) -> state().
finish_pipeline(Stage, #state{jobinfo = #jobinfo{jobname      = JobName,
                                                 save_info    = SaveInfo,
                                                 save_results = Save}} = S) ->
    {_, Outputs} = lists:unzip(stage_outputs(Stage, S)),
    Results = [pipeline_utils:output_urls(O)
               || {_Id, O} <- lists:flatten(Outputs)],
    case Save of
        false ->
            lager:info("Job ~s done, results: ~p", [JobName, Results]),
            event_server:job_done_event(JobName, Results),
            gen_server:cast(self(), pipeline_done);
        true ->
            case lists:prefix("hdfs", SaveInfo) of
                true ->
                    lager:info("Job ~s done, results saved to hdfs.", [JobName]),
                    event_server:job_done_event(JobName, [Results]);
                false ->
                    save_ddfs(JobName, Results)
            end
    end,
    S.

-spec save_ddfs(jobname(), [[url()]]) -> ok.
save_ddfs(JobName, Results) ->
    Tag = list_to_binary(disco:format("disco:results:~s", [JobName])),
    T = <<"tag://", Tag/binary>>,
    Op = {put, urls, Results, internal},
    case ddfs_master:tag_operation(Op, Tag) of
        {ok, TagReps} ->
            lager:info("Job ~s done, results saved to ~p at ~p",
                       [JobName, T, TagReps]),
            event_server:job_done_event(JobName, [T]);
        E ->
            M = "Job ~s failed to save results to ~s: ~p",
            MArgs = [JobName, T, E],
            event_server:event(JobName, M, MArgs, none)
    end.

-spec retry_task(host(), term(), task_info(), state()) -> state().
retry_task(Host, _Error,
           #task_info{spec = #task_spec{jobname = JobName,
                                        taskid  = TaskId,
                                        stage   = Stage},
                      failed_count = FailedCnt,
                      failed_hosts = FH} = TInfo,
           #state{tasks = Tasks,
                  jehandler = JEHandler,
                  hosts = Cluster} = S) ->
    {ok, MaxFail} = application:get_env(max_failure_rate),
    FC = FailedCnt + 1,
    case FC > MaxFail of
        true ->
            M = disco:format("Task ~s:~B failed ~B times."
                             " At most ~B failures are allowed.",
                             [Stage, TaskId, FC, MaxFail]),
            job_event:task_event(JEHandler, {JobName, Stage, TaskId}, {<<"ERROR">>, M}),
            kill_job(M),
            S;
        false ->
            JobCoord = self(),
            _ = spawn_link(
                  fun() ->
                          Sleep =
                              lists:min([FC * ?FAILED_MIN_PAUSE,
                                         ?FAILED_MAX_PAUSE])
                              + random:uniform(?FAILED_PAUSE_RANDOMIZE),
                          M = "Task ~s:~B failed on ~p, ~Bth failures so far."
                              " Sleeping ~B seconds before retrying.",
                          MArgs = [Stage, TaskId, Host, FC, round(Sleep / 1000)],
                          event_server:event(JobName, M, MArgs, none),
                          timer:sleep(Sleep),
                          submit_tasks(JobCoord, re_run, [TaskId])
                  end),
            % We need to update the task's host blacklist.  If we've
            % already exhausted all cluster hosts, reset the
            % blacklist.
            FailedHosts = gb_sets:add(Host, FH),
            Blacklist =
                case gb_sets:is_empty(gb_sets:subtract(Cluster, FailedHosts)) of
                    true  -> gb_sets:empty();
                    false -> FailedHosts
                end,
            TInfo1 = TInfo#task_info{failed_count = FC,
                                     failed_hosts = Blacklist},
            S#state{tasks = jc_utils:update_task_info(TaskId, TInfo1, Tasks)}
    end.

-spec do_kill_job(term(), state()) -> ok.
do_kill_job(Reason, #state{jobinfo = #jobinfo{jobname = JobName}}) ->
    lager:info("Job ~s failed: ~p", [JobName, Reason]),
    disco_server:kill_job(JobName, ?DISCO_SERVER_TIMEOUT),
    event_server:end_job(JobName).

-spec regenerate_input(task_info(), input_id(), data_info(), state()) -> state().
regenerate_input(_WaiterTInfo, {GenTaskId, _} = _InputId,
                 #data_info{failures = Failures} = _DInfo,
                 S) ->
    % We need to re-run the GenTask that generated the specified
    % input, and do this on a host different from the known failing
    % hosts; the current task needs to wait for this regenerating task
    % to complete before it can be retried.
    FailingHosts = gb_sets:from_list(gb_trees:keys(Failures)),
    % Backtrack through the task dependency graph and get the runnable
    % tasks from all the tasks that need to be re-run.
    {TaskIdsToRun, S1} = collect_runnable_deps(GenTaskId, FailingHosts, S),
    do_submit_tasks(re_run, TaskIdsToRun, S1).

-spec task_complete(task_id(), host(), [task_output()], state()) -> state().
task_complete(TaskId, Host, Outputs, S) ->
    #state{tasks = Tasks, pipeline = P} = S1 =
        wakeup_waiters(TaskId, Host, Outputs, S),
    #task_info{spec = #task_spec{stage = Stage}} = jc_utils:task_info(TaskId, Tasks),
    #state{stage_info = SI} = S2 = submit_concurrent_tasks(S1, Stage, TaskId, Outputs),
    case can_finish(P, Stage, SI) of
        true -> stage_done(Stage);
        false -> ok
    end,
    maybe_start_pending(S2).

-spec maybe_start_pending(state()) -> state().
maybe_start_pending(#state{pending = Pending} = S) ->
    gb_sets:fold(
        fun({TaskId, Mode}, #state{schedule = Schedule} = S1) ->
            #state{tasks = Tasks, pipeline = P, stage_info = SI} = S1,
            #task_info{spec = TaskSpec} = jc_utils:task_info(TaskId, Tasks),
            #task_spec{stage = Stage} = TaskSpec,
            case jc_utils:can_run_task(P, Stage, SI, Schedule) of
                true  -> do_submit_tasks(Mode, [TaskId], S1);
                false -> S1
            end
        end, S, Pending).

-spec send_outputs_to_consumers(state(), [grouped_output()], stage_name()) -> state().
send_outputs_to_consumers(#state{stage_info = SI} = S, ModifiedGroups, Stage) ->
    #stage_info{group_map = GroupMap} = jc_utils:stage_info(Stage, SI),
    lists:foldl(fun({G, _} = GroupedInputs, S1) ->
                TaskId = gb_trees:get(G, GroupMap),
                send_outputs_to_consumer(S1, TaskId, GroupedInputs)
        end, S, ModifiedGroups).

update_taskspec(#state{tasks = Tasks} = S, TaskId, Fun) ->
    #task_info{spec = TaskSpec} = TaskInfo = jc_utils:task_info(TaskId, Tasks),
    Tasks1 = jc_utils:update_task_info(TaskId,
        TaskInfo#task_info{spec=Fun(TaskSpec)}, Tasks),
    S#state{tasks = Tasks1}.

-spec add_inputs_to_spec(state(), task_id(), [{input_id(), data_input()}]) -> state().
add_inputs_to_spec(S, TaskId, NewInputs) ->
    InputIds = [Id|| {Id,_} <- NewInputs],
    S1 = update_taskspec(S, TaskId,
        fun(TaskSpec) ->
            CurrentInputs = TaskSpec#task_spec.input,
            Set1 = gb_sets:from_list(CurrentInputs),
            Set2 = gb_sets:from_list(InputIds),
            UnionSet = gb_sets:union(Set1, Set2),
            TaskSpec#task_spec{input = gb_sets:to_list(UnionSet)}
        end),
    add_inputs_to_data_map(S1, NewInputs).

-spec mark_task_inputs_done(state(), task_id()) -> state().
mark_task_inputs_done(S, TaskId) ->
    update_taskspec(S, TaskId,
        fun(TaskSpec) ->
            TaskSpec#task_spec{all_inputs = true}
        end).

-spec send_outputs_to_consumer(state(), task_id(), grouped_output()) -> state().
send_outputs_to_consumer(S, TaskId, {_, Inputs}) ->
    #state{tasks = Tasks} = S1 = add_inputs_to_spec(S, TaskId, Inputs),
    TaskInfo = jc_utils:task_info(TaskId, Tasks),
    W = TaskInfo#task_info.worker,
    case W of
        none ->
            Tasks1 = jc_utils:update_task_info(TaskId,
                TaskInfo#task_info{new_input=true}, Tasks),
            S1#state{tasks = Tasks1};
        _    ->
            disco_worker:add_inputs(W, Inputs),
            S1
    end.

-spec submit_concurrent_tasks(state(), stage_name(), task_id(), [task_output()]) -> state().
submit_concurrent_tasks(#state{stage_info = SI, pipeline = P} = S, Stage, TaskId, Outputs) ->
    case pipeline_utils:next_stage(P, Stage) of
        done ->
            % there is no tasks in the next stage to be started.
            S#state{stage_info = jc_utils:update_stage_tasks(Stage, TaskId, done, SI)};
        {_, _, false} ->
            S#state{stage_info = jc_utils:update_stage_tasks(Stage, TaskId, done, SI)};
        {Next, Grouping, true} ->
            case jc_utils:stage_info_opt(Stage, SI) of
                none ->
                    PrevStageOutputs = [{TaskId, Outputs}],
                    NewGroups = pipeline_utils:group_outputs(Grouping, PrevStageOutputs),
                    ModifiedGroups = [];
                _ ->
                    PrevStageOutputs = stage_outputs(Stage, S),
                    {NewGroups, ModifiedGroups} =
                        pipeline_utils:get_grouping_lists(Grouping,
                                                          PrevStageOutputs,
                                                          TaskId, Outputs)
            end,
            S1 = S#state{stage_info = jc_utils:update_stage_tasks(Stage, TaskId, done, SI)},
            {NTasks, S2} = make_stage_tasks(Next, Grouping, NewGroups, S1, {0, []}),
            S3 = do_submit_tasks(first_run, NTasks, S2),
            send_outputs_to_consumers(S3, ModifiedGroups, Next)
    end.

wakeup_waiters(TaskId, Host, Outputs, #state{tasks = Tasks} = S) ->
    #task_info{failed_hosts = FH,
               waiters = Waiters}
        = TInfo = jc_utils:task_info(TaskId, Tasks),
    TInfo1 = TInfo#task_info{failed_hosts = gb_sets:delete_any(Host, FH),
                             worker  = none,
                             waiters = [],
                             outputs = Outputs},
    {Awake, Tasks1} = jc_utils:wakeup_waiters(TaskId, Waiters, Tasks),
    S1 = do_submit_tasks(re_run, Awake, S),
    S1#state{tasks = jc_utils:update_task_info(TaskId, TInfo1, Tasks1)}.

-spec do_stage_done(stage_name(), state()) -> state().
do_stage_done(Stage, S) ->
    maybe_event_stage_done(Stage, S),
    S1 = mark_stage_finished(Stage, S),
    S2 = do_next_stage(Stage, S1),
    maybe_start_pending(S2).

-spec maybe_event_stage_done(stage_name(), state()) -> ok.
maybe_event_stage_done(Stage, #state{pipeline = P, stage_info = SI} = S) ->
    case can_finish(P, Stage, SI) of
        true -> event_stage_done(Stage, S);
        false ->
            lager:info("Pipeline ~p, StageInfo ~p, Stage ~p", [P, SI, Stage]),
            ok
    end.

-spec can_finish(pipeline(), stage_name(), disco_gbtree(stage_name(), stage_info())) -> boolean().
can_finish(P, Stage, SI) ->
    jc_utils:no_tasks_running(Stage, SI) andalso
    pipeline_utils:all_deps_finished(P, Stage, SI) andalso
    jc_utils:last_stage_task(Stage, SI).

-spec event_stage_done(stage_name(), state()) -> ok.
event_stage_done(Stage, #state{jobinfo    = #jobinfo{jobname = JobName},
                            tasks      = Tasks,
                            stage_info = SI}) ->
    case Stage of
        ?INPUT ->
            ok;
        _ ->
            #stage_info{start = Start, done = Done}
                = jc_utils:stage_info(Stage, SI),
            STasks = [jc_utils:task_info(TaskId, Tasks) || TaskId <- Done],
            Results = [pipeline_utils:output_urls(O)
                       || T <- STasks, {_Id, O} <- T#task_info.outputs],
            Event = {stage_ready, Stage, Results},
            Since = disco:format_time_since(Start),
            event_server:event(JobName, "Stage ~s finished in ~s",
                               [Stage, Since], Event)
    end.

-spec mark_stage_finished(stage_name(), state()) -> state().
mark_stage_finished(Stage, #state{stage_info = SI} = S) ->
    StageInfo0 = jc_utils:stage_info(Stage, SI),
    StageInfo = StageInfo0#stage_info{finished = true},
    SI1 = jc_utils:update_stage(Stage, StageInfo, SI),
    S#state{stage_info = SI1}.

-spec do_next_stage(stage_name(), state()) -> state().
do_next_stage(Stage, #state{pipeline = P, stage_info = SI} = S) ->
    case pipeline_utils:next_stage(P, Stage) of
        done ->
            case can_finish(P, Stage, SI) of
                true -> finish_pipeline(Stage, S);
                false -> S
            end;
        {Next, Grouping, false} ->
            start_next_stage(stage_outputs(Stage, S), Next, Grouping, S);
        {Next, Grouping, true} ->
            S1 = send_termination_signal(Next, S),
            case pipeline_utils:group_outputs(Grouping, stage_outputs(Stage, S1)) of
                [] -> start_next_stage([], Next, Grouping, S1);
                _  -> S1
            end
    end.

-spec send_term_signal_to_task(task_id(), state()) -> state().
send_term_signal_to_task(TaskId, S) ->
    #state{tasks = Tasks} = S1 = mark_task_inputs_done(S, TaskId),
    TaskInfo = jc_utils:task_info(TaskId, Tasks),
    case TaskInfo#task_info.worker of
        none ->
            Tasks1 = jc_utils:update_task_info(TaskId,
                TaskInfo#task_info{end_input=true}, Tasks),
            S1#state{tasks = Tasks1};
        W ->
            disco_worker:terminate_inputs(W),
            S1
    end.

-spec send_termination_signal(stage_name(), state()) -> state().
send_termination_signal(Stage, #state{stage_info = SI, pending = Pending} = S) ->
    S2 = lists:foldl(fun(TaskId, S1) ->
                    send_term_signal_to_task(TaskId, S1)
                end, S, jc_utils:running_tasks(Stage, SI)),
    gb_sets:fold(fun({TaskId, _Mod}, #state{tasks = Tasks} = S3) ->
                    #task_info{spec = TaskSpec} = jc_utils:task_info(TaskId, Tasks),
                    #task_spec{stage = TaskStage} = TaskSpec,
                    case TaskStage of
                        Stage -> send_term_signal_to_task(TaskId, S3);
                        _     -> S3
                    end
                end, S2, Pending).

-spec start_next_stage([{task_id(), [task_output()]}], stage_name(),
                       label_grouping(), state()) -> state().
start_next_stage(PrevStageOutputs, Stage, Grouping,
                 #state{jobinfo = #jobinfo{jobname = JobName}} = S) ->
    {Tasks, S1} = setup_stage_tasks(PrevStageOutputs, Stage, Grouping, S),
    case Tasks of
        [] ->
            do_stage_done(Stage, S1);
        [_|_] ->
            NTasks = length(Tasks),
            Event = {stage_start, Stage, NTasks},
            event_server:event(JobName, "Stage ~s scheduled with ~p tasks",
                               [Stage, NTasks], Event),
            do_submit_tasks(first_run, Tasks, S1)
    end.

-spec setup_stage_tasks([{task_id(), [task_output()]}], stage_name(), label_grouping(),
                        state()) -> {[task_id()], state()}.
setup_stage_tasks(PrevStageOutputs, Stage, Grouping, S) ->
    % The outputs of the previous stage are grouped in the specified
    % way, and these grouped outputs form the inputs for the current
    % stage.
    GOutputs = pipeline_utils:group_outputs(Grouping, PrevStageOutputs),
    make_stage_tasks(Stage, Grouping, GOutputs, S, {0, []}).

-spec make_stage_tasks(stage_name(), label_grouping(),
                       [grouped_output()], state(),
                       {non_neg_integer(), [task_id()]}) -> {[task_id()], state()}.
make_stage_tasks(Stage, _Grouping, [],
                 #state{stage_info = SI} = S, {TaskNum, Tasks}) ->
    StageInfo1 = case jc_utils:stage_info_opt(Stage, SI) of
        none ->
            #stage_info{start = now(), all = TaskNum};
        #stage_info{all = All} = StageInfo ->
            StageInfo#stage_info{all = All + TaskNum}
    end,
    SI1 = jc_utils:update_stage(Stage, StageInfo1, SI),
    {Tasks, S#state{stage_info = SI1}};
make_stage_tasks(Stage, Grouping, [{G, Inputs}|Rest],
                 #state{jobinfo = #jobinfo{jobname = JN,
                                           jobenvs = JE,
                                           save_info = SaveInfo,
                                           save_results = Save,
                                           worker  = W},
                        pipeline   = P,
                        tasks = Tasks,
                        schedule    = Schedule,
                        next_taskid = NextTaskId,
                        jehandler   = JEHandler,
                        stage_info = SI} = S,
                 {TaskNum, Acc}) ->
    S1 = add_inputs_to_data_map(S, Inputs),
    {InputIds, _DataInputs} = lists:unzip(Inputs),
    SaveOutputs =
        (pipeline_utils:next_stage(P, Stage) == done) andalso Save,

    AllInputs = case Grouping of
        split -> true;
        _     -> pipeline_utils:all_deps_finished(P, Stage, SI)
    end,
    TaskSpec = #task_spec{jobname = JN,
                          jehandler = JEHandler,
                          stage   = Stage,
                          taskid  = NextTaskId,
                          tasknum = TaskNum,
                          group   = G,
                          jobenvs = JE,
                          worker  = W,
                          input   = InputIds,
                          all_inputs = AllInputs,
                          grouping  = Grouping,
                          job_coord = self(),
                          schedule  = Schedule,
                          save_outputs = SaveOutputs,
                          save_info = SaveInfo},

    #stage_info{group_map = OldGroupMap} = StageInfo =
    case jc_utils:stage_info_opt(Stage, SI) of
        none         -> #stage_info{start = now()};
        StageInfoTmp -> StageInfoTmp
    end,
    GroupMap = case Grouping of
        split -> OldGroupMap;
        _     -> gb_trees:insert(G, NextTaskId, OldGroupMap)
    end,
    StageInfo1 = StageInfo#stage_info{group_map = GroupMap},

    S2 = S1#state{next_taskid = NextTaskId + 1,
                  stage_info = jc_utils:update_stage(Stage, StageInfo1, SI),
                 tasks = jc_utils:add_task_spec(NextTaskId, TaskSpec, Tasks)},
    make_stage_tasks(Stage, Grouping, Rest, S2,
                     {TaskNum + 1, [NextTaskId | Acc]}).

-spec add_inputs_to_data_map(state(), [{input_id(), data_input()}]) -> state().
add_inputs_to_data_map(#state{data_map = OldDataMap} = S, Inputs) ->
    DataMap = lists:foldl(
                fun({InputId, DataInput}, DM) ->
                        DataHosts = pipeline_utils:locations(DataInput),
                        Locations = lists:sort([{H, DataInput}
                                                || H <- DataHosts]),
                        Failures  = lists:sort([{H, 0} || H <- DataHosts]),
                        DInfo = #data_info{source = DataInput,
                                           locations =
                                               gb_trees:from_orddict(Locations),
                                           failures =
                                               gb_trees:from_orddict(Failures)},
                        jc_utils:add_input(InputId, DInfo, DM)
                end, OldDataMap, Inputs),
    S#state{data_map = DataMap}.


-spec do_submit_tasks(submit_mode(), [task_id()], state()) -> state().
do_submit_tasks(_Mode, [], S) -> S;
do_submit_tasks(Mode, [TaskId | Rest], #state{stage_info = SI,
                                              pipeline   = P,
                                              pending    = Pending,
                                              schedule = Schedule,
                                              jehandler = JEHandler,
                                              tasks      = Tasks} = S) ->
    #task_info{spec = TaskSpec} = jc_utils:task_info(TaskId, Tasks),
    #task_spec{stage = Stage} = TaskSpec,
    case jc_utils:can_run_task(P, Stage, SI, Schedule) of
        false ->
            job_event:pending_event(JEHandler, Stage, add),
            do_submit_tasks(Mode, Rest,
                            S#state{pending = gb_sets:add_element({TaskId, Mode}, Pending)});
        true ->
            NewPending = case gb_sets:is_element({TaskId, Mode}, Pending) of
                true ->
                    job_event:pending_event(JEHandler, Stage, remove),
                    gb_sets:del_element({TaskId, Mode}, Pending);
                false -> Pending
            end,
            S1 = S#state{pending = NewPending},
            do_submit_tasks_in(Mode, [TaskId|Rest], S1)
    end.

-spec do_submit_tasks_in(submit_mode(), [task_id()], state()) -> state().
do_submit_tasks_in(Mode, [TaskId | Rest], #state{stage_info = SI,
                                              data_map   = DataMap,
                                              next_runid = RunId,
                                              hosts      = Hosts,
                                              tasks      = Tasks} = S) ->
    #task_info{spec = TaskSpec, failed_hosts = FailedHosts}
        = jc_utils:task_info(TaskId, Tasks),
    #task_spec{stage = Stage, group = {_L, H}, input = Input} = TaskSpec,
    Inputs = jc_utils:task_inputs(Input, DataMap),
    % On first_run, we use host selected by grouping, if it belongs to
    % the cluster; otherwise, we let the host-allocator choose it.
    Host = case {Mode, gb_sets:is_member(H, Hosts)} of
               {first_run, true}  -> H;
               {_,            _}  -> none
           end,
    TaskRun = #task_run{runid  = RunId,
                        host   = Host,
                        input  = Inputs,
                        failed_hosts = FailedHosts},
    % There is no back pressure from disco_server on the job_coordinator, so we
    % will have to trust that the job_coordinator is not going to cause
    % problems for the disco_server (if there are too many tasks, we have
    % to use max_cores to limit the pressure on disco_server).
    disco_server:new_task({TaskSpec, TaskRun}),
    SI1 = jc_utils:update_stage_tasks(Stage, TaskId, run, SI),
    do_submit_tasks(Mode, Rest, S#state{next_runid = RunId+1, stage_info=SI1}).

% This returns the list of runnable dependency tasks, and an updated
% state.
-spec collect_runnable_deps(task_id(), disco_gbset(host()), state())
                           -> {[task_id()], state()}.
collect_runnable_deps(TaskId, FHosts, #state{tasks      = Tasks,
                                             stage_info = SI,
                                             data_map   = DataMap} = S) ->
    {RunIds, Tasks2} =
        jc_utils:collect_stagewise(TaskId, Tasks, SI, DataMap, FHosts),
    {gb_sets:to_list(RunIds), S#state{tasks = Tasks2}}.
