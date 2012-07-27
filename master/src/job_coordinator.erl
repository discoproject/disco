-module(job_coordinator).
-behaviour(gen_server).

-export([new/1, task_done/2, update_nodes/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("config.hrl").
-include("pipeline.hrl").
-include("job_coordinator.hrl").

-define(TASK_SUBMIT_TIMEOUT, 30000).
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
    case gen_server:start_link(?MODULE, {Starter, JobPack}, []) of
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

-spec kill_job(term()) -> ok.
kill_job(Reason) ->
    gen_server:cast(self(), {kill_job, Reason}).

-spec stage_done(stage_name()) -> ok.
stage_done(Stage) ->
    gen_server:cast(self(), {stage_done, Stage}).

%% ===================================================================
%% gen_server callbacks

% Internal state of the job coordinator.
-record(state, {jobinfo          :: jobinfo(),
                pipeline         :: pipeline(),
                schedule         :: task_schedule(),
                next_taskid = 0  :: task_id(),
                next_runid  = 0  :: task_run_id(),
                % cluster membership: [host()]
                hosts      = gb_sets:empty()  :: gb_set(),
                % input | task_id() -> task_info().
                tasks      = gb_trees:empty() :: gb_tree(),
                % input_id() -> data_info().
                data_map   = gb_trees:empty() :: gb_tree(),
                % stage_name() -> stage_info().
                stage_info = gb_trees:empty() :: gb_tree()}).
-type state() :: #state{}.

-spec init({pid(), binary()}) -> gs_init() | {stop, term()}.
init({Starter, JobPack}) ->
    try  {JobInfo, Hosts} = setup_job(JobPack, self()),
         Starter ! {job_started, JobInfo#jobinfo.jobname},
         {ok, init_state(JobInfo, Hosts)}
    catch
        {error, E} ->
            {stop, E};
        K:V ->
            {stop, disco:format("job init error: ~p:~p", [K, V])}
    end.

-spec handle_call(term(), from(), state()) -> gs_noreply().
handle_call(_M, _F, S) ->
    {noreply, S}.

-spec handle_cast({update_nodes, [host()]}, state()) -> gs_noreply();
                 ({stage_done, stage_name()}, state()) -> gs_noreply();
                 ({submit_tasks, submit_mode(), [task_id()]}, state()) ->
                         gs_noreply();
                 ({task_done, task_id(), host(), [task_output()]}, state()) ->
                         gs_noreply();
                 (pipeline_done, state()) -> gs_noreply();
                 ({kill_job, term()}, state()) -> gs_noreply().
handle_cast({update_nodes, Hosts}, S) ->
    {noreply, do_update_nodes(Hosts, S)};
handle_cast({submit_tasks, Mode, Tasks}, S) ->
    {noreply, do_submit_tasks(Mode, Tasks, S)};
handle_cast({stage_done, Stage}, S) ->
    {noreply, do_stage_done(Stage, S)};
handle_cast({task_done, TaskId, Host, Results}, S) ->
    {noreply, do_task_done(TaskId, Host, Results, S)};
handle_cast(pipeline_done, S) ->
    {stop, normal, S};
handle_cast({kill_job, Reason}, S) ->
    do_kill_job(Reason, S),
    {stop, Reason, S}.

-spec handle_info(term(), state()) -> gs_noreply().
handle_info(_M, S) ->
    {noreply, S}.

-spec terminate(term(), state()) -> ok.
terminate(normal, _S) -> ok;
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
    case disco_server:new_job(JobName, JobCoord, 30000) of
        ok -> ok;
        {error, _E} = T1 -> throw(T1)
    end,
    {JobInfo#jobinfo{jobname = JobName, jobfile = JobFile}, Hosts}.

-spec init_state(jobinfo(), [host()]) -> state().
init_state(#jobinfo{schedule = Schedule,
                    inputs   = Inputs,
                    pipeline = Pipeline} = JobInfo, Hosts) ->
    % Create a dummy completed 'input' task.
    Tasks = gb_trees:from_orddict([{input,
                                    #task_info{spec = input,
                                               outputs = disco:enum(Inputs)}}]),
    % Mark the 'input' stage as done, and send notification.
    InputStage = #stage_info{all = 1, done = [input]},
    SI = gb_trees:from_orddict([{?INPUT, InputStage}]),
    stage_done(?INPUT),
    #state{jobinfo    = JobInfo,
           pipeline   = Pipeline,
           schedule   = Schedule,
           hosts      = gb_sets:from_list(Hosts),
           tasks      = Tasks,
           stage_info = SI}.

%% ===================================================================
%% state access and update utils

-spec stage_outputs(stage_name(), state()) -> [{task_id(), [task_output()]}].
stage_outputs(Stage, #state{stage_info = SI, tasks = Tasks}) ->
    Done = (jc_utils:stage_info(Stage, SI))#stage_info.done,
    [{Id, jc_utils:task_outputs(Id, Tasks)} || Id <- Done].

%% ===================================================================
%% Callback implementations.

-spec do_update_nodes([host()], state()) -> state().
do_update_nodes(Hosts, S) ->
    S#state{hosts = gb_sets:from_list(Hosts)}.

-spec do_task_done(task_id(), host(), task_done_result(), state()) -> state().
do_task_done(TaskId, Host, Result, #state{jobinfo = #jobinfo{jobname = JobName},
                                          tasks   = Tasks,
                                          data_map   = DataMap,
                                          stage_info = SI} = S) ->
    #task_info{spec = #task_spec{stage = Stage}}
        = TInfo = jc_utils:task_info(TaskId, Tasks),
    ETInfo = {JobName, Stage, TaskId},
    FEvent = {task_failed, Stage},
    case Result of
        {fatal, F} ->
            event_server:task_event(ETInfo, {<<"FATAL">>, F}, FEvent),
            kill_job(F),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            S#state{stage_info = SI1};
        {error, E} ->
            event_server:task_event(ETInfo, {<<"WARNING">>, E}, FEvent),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            retry_task(Host, E, TInfo, S#state{stage_info = SI1});
        {input_error, {{input, _}, _}} = E ->
            % If a pipeline input is inaccessible, we cannot re-run
            % the generating task since there isn't one, so we
            % fallback to retry-ing the task.
            event_server:task_event(ETInfo, {<<"WARNING">>, "Job input failed"},
                                    FEvent),
            SI1 = jc_utils:update_stage_tasks(Stage, TaskId, stop, SI),
            retry_task(Host, E, TInfo, S#state{stage_info = SI1});
        {input_error, {InputId, InputHosts}} = E ->
            event_server:task_event(ETInfo, {<<"WARNING">>, "Input failed"},
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
            event_server:task_event(ETInfo,
                                    disco:format("Received results from ~s", [Host]),
                                    {task_ready, Stage}),
            task_complete(TaskId, Host, Outputs, S)
    end.

-spec finish_pipeline(stage_name(), state()) -> ok.
finish_pipeline(Stage, #state{jobinfo = #jobinfo{jobname = JobName},
                              tasks   = Tasks,
                              stage_info = SI}) ->
    #stage_info{done = Done} = jc_utils:stage_info(Stage, SI),
    Outputs = [(jc_utils:task_info(TaskId, Tasks))#task_info.outputs
               || TaskId <- Done],
    Results = [pipeline_utils:output_urls(O)
               || {_Id, O} <- lists:flatten(Outputs)],
    lager:info("Job ~s done, results: ~p", [JobName, Results]),
    event_server:job_done_event(JobName, Results),
    gen_server:cast(self(), pipeline_done).

-spec retry_task(host(), term(), task_info(), state()) -> state().
retry_task(Host, _Error,
           #task_info{spec = #task_spec{jobname = JobName,
                                        taskid  = TaskId,
                                        stage   = Stage},
                      failed_count = FailedCnt,
                      failed_hosts = FH} = TInfo,
           #state{tasks = Tasks} = S) ->
    {ok, MaxFail} = application:get_env(max_failure_rate),
    FC = FailedCnt + 1,
    case FC > MaxFail of
        true ->
            M = disco:format("Task ~s:~B failed ~B times."
                             " At most ~B failures are allowed.",
                             [Stage, TaskId, FC, MaxFail]),
            event_server:task_event({JobName, Stage, TaskId}, {<<"ERROR">>, M}),
            kill_job(M);
        false ->
            JobCoord = self(),
            _ = spawn_link(
                  fun() ->
                          Sleep =
                              lists:min([FC * ?FAILED_MIN_PAUSE,
                                         ?FAILED_MAX_PAUSE])
                              + random:uniform(?FAILED_PAUSE_RANDOMIZE),
                          M = "Task ~s:~B failed for the ~Bth time."
                              " Sleeping ~B seconds before retrying.",
                          MArgs = [Stage, TaskId, FC, round(Sleep / 1000)],
                          event_server:event(JobName, M, MArgs, none),
                          timer:sleep(Sleep),
                          submit_tasks(JobCoord, re_run, [TaskId])
                  end),
            TInfo1 = TInfo#task_info{failed_count = FC,
                                     failed_hosts = gb_sets:add(Host, FH)},
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
task_complete(TaskId, Host, Outputs, #state{tasks      = Tasks,
                                            stage_info = SI} = S) ->
    #task_info{failed_hosts = FH,
               waiters = Waiters,
               spec = #task_spec{stage = Stage}}
        = TInfo = jc_utils:task_info(TaskId, Tasks),
    TInfo1 = TInfo#task_info{failed_hosts = gb_sets:delete_any(Host, FH),
                             worker  = none,
                             waiters = [],
                             outputs = Outputs},
    % Get the runnable set of waiters.
    {Awake, Tasks1} = jc_utils:wakeup_waiters(TaskId, Waiters, Tasks),
    % Dispatch next stage if this was the last task to finish in this
    % stage.
    StageDone = jc_utils:last_stage_task(Stage, TaskId, SI),
    case StageDone of
        true  -> stage_done(Stage);
        false -> ok
    end,
    S1 = S#state{stage_info = jc_utils:update_stage_tasks(Stage, TaskId, done, SI),
                 tasks = jc_utils:update_task_info(TaskId, TInfo1, Tasks1)},
    do_submit_tasks(re_run, Awake, S1).

-spec do_stage_done(stage_name(), state()) -> state().
do_stage_done(Stage, #state{jobinfo    = #jobinfo{jobname = JobName},
                            pipeline   = P,
                            tasks      = Tasks,
                            stage_info = SI} = S) ->
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
    end,
    case pipeline_utils:next_stage(P, Stage) of
        done ->
            finish_pipeline(Stage, S),
            S;
        {Next, Grouping} ->
            % If this is the first time this stage has finished, then
            % we need to start the tasks in the next stage.
            case jc_utils:stage_info_opt(Next, SI) of
                none -> start_next_stage(Stage, Next, Grouping, S);
                _    -> S
            end
    end.
start_next_stage(Prev, Stage, Grouping, S) ->
    {Tasks, S1} = setup_stage_tasks(Prev, Stage, Grouping, S),
    case Tasks of
        []    -> do_stage_done(Stage, S1);
        [_|_] -> do_submit_tasks(first_run, Tasks, S1)
    end.

-spec setup_stage_tasks(stage_name(), stage_name(), label_grouping(), state())
                       -> {[task_id()], state()}.
setup_stage_tasks(Prev, Stage, Grouping, S) ->
    % The outputs of the previous stage are grouped in the specified
    % way, and these grouped outputs form the inputs for the current
    % stage.
    Outputs = stage_outputs(Prev, S),
    GOutputs = pipeline_utils:group_outputs(Grouping, Outputs),
    make_stage_tasks(Stage, Grouping, GOutputs, S, []).

make_stage_tasks(Stage, _Grouping, [], #state{stage_info = SI} = S, Tasks) ->
    StageInfo = #stage_info{start = now(), all = length(Tasks)},
    SI1 = jc_utils:update_stage(Stage, StageInfo, SI),
    {Tasks, S#state{stage_info = SI1}};
make_stage_tasks(Stage, Grouping, [{G, Inputs}|Rest],
                 #state{jobinfo = #jobinfo{jobname = JN,
                                           jobenvs = JE,
                                           worker  = W},
                        tasks = Tasks,
                        schedule    = Schedule,
                        next_taskid = NextTaskId,
                        data_map    = OldDataMap} = S,
                 Acc) ->
    DataMap = lists:foldl(
                fun({InputId, DataInput}, DM) ->
                        DataHosts = pipeline_utils:locations(DataInput),
                        Locations = lists:sort([{H, DataInput} || H <- DataHosts]),
                        Failures  = lists:sort([{H, 0} || H <- DataHosts]),
                        DInfo = #data_info{source = DataInput,
                                           locations = gb_trees:from_orddict(Locations),
                                           failures = gb_trees:from_orddict(Failures)},
                        jc_utils:add_input(InputId, DInfo, DM)
                end, OldDataMap, Inputs),
    {InputIds, _DataInputs} = lists:unzip(Inputs),
    TaskSpec = #task_spec{jobname = JN,
                          stage   = Stage,
                          taskid  = NextTaskId,
                          group   = G,
                          jobenvs = JE,
                          worker  = W,
                          input   = InputIds,
                          grouping  = Grouping,
                          job_coord = self(),
                          schedule  = Schedule},

    S1 = S#state{next_taskid = NextTaskId + 1,
                 data_map    = DataMap,
                 tasks = jc_utils:add_task_spec(NextTaskId, TaskSpec, Tasks)},
    make_stage_tasks(Stage, Grouping, Rest, S1, [NextTaskId | Acc]).

-spec do_submit_tasks(submit_mode(), [task_id()], state()) -> state().
do_submit_tasks(_Mode, [], S) -> S;
do_submit_tasks(Mode, [TaskId | Rest], #state{stage_info = SI,
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
               {_,         false} -> none
           end,
    TaskRun = #task_run{runid  = RunId,
                        host   = Host,
                        input  = Inputs,
                        failed_hosts = FailedHosts},
    % TODO: retry submission on submission failure.  For now, assert
    % ok.
    ok = disco_server:new_task({TaskSpec, TaskRun}, ?TASK_SUBMIT_TIMEOUT),
    SI1 = jc_utils:update_stage_tasks(Stage, TaskId, run, SI),
    do_submit_tasks(Mode, Rest, S#state{next_runid = RunId + 1,
                                        stage_info = SI1}).

% This returns the list of runnable dependency tasks, and an updated
% state.
-spec collect_runnable_deps(task_id(), gb_set(), state())
                           -> {[task_id()], state()}.
collect_runnable_deps(TaskId, FHosts, #state{tasks      = Tasks,
                                             stage_info = SI,
                                             data_map   = DataMap} = S) ->
    {RunIds, Tasks2} =
        jc_utils:collect_stagewise(TaskId, Tasks, SI, DataMap, FHosts),
    {gb_sets:to_list(RunIds), S#state{tasks = Tasks2}}.
