
-module(job_coordinator).
-export([new/1]).

-include("disco.hrl").
-include("config.hrl").

% In theory we could keep the HTTP connection pending until the job
% finishes but in practice long-living HTTP connections are a bad idea.
% Thus, the HTTP request spawns a new process, job_coordinator, that
% takes care of coordinating the whole map-reduce show, including
% fault-tolerance. The HTTP request returns immediately. It may poll
% the job status e.g. by using handle_ctrl's get_results.
-spec new(binary()) -> {'ok', _}.
new(JobPack) ->
    Self = self(),
    spawn_link (fun() -> job_coordinator(Self, JobPack) end),
    receive
        {job_submitted, JobName} ->
            {ok, JobName}
    after 30000 ->
            exit("couldn't start a new job in 30s (master busy?)")
    end.

job_event(JobName, {EventFormat, Args, Params}) ->
    event_server:event(JobName, EventFormat, Args, Params);
job_event(JobName, {EventFormat, Args}) ->
    job_event(JobName, {EventFormat, Args, {}});
job_event(JobName, Event) ->
    job_event(JobName, {Event, [], {}}).

-spec job_coordinator(nonempty_string(), [binary() | jobinfo()]) -> 'ok'.
job_coordinator(JobName, #jobinfo{} = Job) ->
    job_event(JobName, {"Starting job", [], {job_data, Job}}),
    Started = now(),
    RedInputs = if Job#jobinfo.map ->
                        job_event(JobName, "Map phase"),
                        MapResults = run_task(map_input(Job#jobinfo.inputs),
                                              "map",
                                              JobName,
                                              Job),
                        job_event(JobName, {"Map phase done", [], {map_ready, MapResults}}),
                        MapResults;
                   true ->
                        Job#jobinfo.inputs
                end,

    if Job#jobinfo.reduce ->
            job_event(JobName, "Starting reduce phase"),
            RedResults = run_task(reduce_input(JobName,
                                               RedInputs,
                                               Job#jobinfo.nr_reduce),
                                  "reduce",
                                  JobName,
                                  Job#jobinfo{force_local = false,
                                              force_remote = false}),

            job_event(JobName, "Reduce phase done"),
            job_event(JobName, {"READY: Job finished in ~s",
                                [disco:format_time_since(Started)],
                                {ready, RedResults}});
       true ->
            job_event(JobName, {"READY: Job finished in ~s",
                                [disco:format_time_since(Started)],
                                {ready, RedInputs}})
    end,
    event_server:end_job(JobName);

job_coordinator(Parent, JobPack) ->
    {Prefix, JobInfo} = jobpack:jobinfo(JobPack),
    JobName = event_server:new_job(Prefix, self()),
    JobFile = jobpack:save(JobPack, disco:jobhome(JobName)),
    _JobPid = disco_server:new_job(JobName, self()),
    Parent ! {job_submitted, JobName},
    job_coordinator(JobName, JobInfo#jobinfo{jobfile = JobFile}).

% work() is the heart of the map/reduce show. First it distributes tasks
% to nodes. After that, it starts to wait for the results and finally
% returns when it has gathered all the results.

-spec work([{non_neg_integer(), [{binary(), nonempty_string()}]}],
    nonempty_string(), nonempty_string(), non_neg_integer(),
    jobinfo(), gb_tree()) -> {'ok', gb_tree()}.

%. 1. Basic case: Tasks to distribute, maximum number of concurrent tasks (N)
%  not reached.
work([{TaskID, Input}|Inputs], Mode, JobName, N, Job, Res) when N < Job#jobinfo.max_cores ->
    Task = #task{from = self(),
                 taskblack = [],
                 fail_count = 0,
                 force_local = Job#jobinfo.force_local,
                 force_remote = Job#jobinfo.force_remote,
                 jobname = JobName,
                 taskid = TaskID,
                 mode = Mode,
                 input = Input,
                 worker = Job#jobinfo.worker},
    submit_task(Task),
    work(Inputs, Mode, JobName, N + 1, Job, Res);

% 2. Tasks to distribute but the maximum number of tasks are already running.
% Wait for tasks to return. Note that wait_workers() may return with the same
% number of tasks still running, i.e. N = M.
work([_|_] = IArg, Mode, JobName, N, Job, Res) when N >= Job#jobinfo.max_cores ->
    {M, NRes} = wait_workers(N, Res, JobName, Mode),
    work(IArg, Mode, JobName, M, Job, NRes);

% 3. No more tasks to distribute. Wait for tasks to return.
work([], Mode, JobName, N, Job, Res) when N > 0 ->
    {M, NRes} = wait_workers(N, Res, JobName, Mode),
    work([], Mode, JobName, M, Job, NRes);

% 4. No more tasks to distribute, no more tasks running. Done.
work([], _Mode, _JobName, 0, _Job, Res) ->
    {ok, Res}.

% wait_workers receives messages from disco_server:clean_worker() that is
% called when a worker exits.
-spec wait_workers(non_neg_integer(), gb_tree(), nonempty_string(),
    nonempty_string()) -> {non_neg_integer(), gb_tree()}.

% Error condition: should not happen.
wait_workers(0, _Res, _JobName, _Mode) ->
    throw("Nothing to wait");
wait_workers(N, Results, _JobName, Mode) ->
    receive
        {{done, Result}, Task, Host} ->
            event_server:task_event(Task,
                                    disco:format("Received results from ~s", [Host]),
                                    {task_ready, Mode}),
            {N - 1, gb_trees:enter(Task#task.taskid,
                                   {disco:node(Host), list_to_binary(Result)},
                                   Results)};
        {{error, Error}, Task, Host} ->
            event_server:task_event(Task, {<<"WARN">>, Error}, {task_failed, Task#task.mode}),
            handle_data_error(Task, Host),
            {N, Results};
        {{fatal, Error}, Task, Host} ->
            Message = disco:format("Worker at '~s' died: ~s", [Host, Error]),
            event_server:task_event(Task, {<<"ERROR">>, Message}, {task_failed, Task#task.mode}),
            throw(logged_error)
    end.

-spec submit_task(task()) -> _.
submit_task(Task) ->
    case catch disco_server:new_task(Task, 30000) of
        ok ->
            ok;
        _ ->
            event_server:event(Task#task.jobname,
            "ERROR: ~s:~B scheduling failed. "
            "Try again later.",
            [Task#task.mode, Task#task.taskid], []),
            throw(logged_error)
    end.

% data_error signals that a task failed on an error that is not likely
% to repeat when the task is ran on another node. The function
% handle_data_error() schedules the failed task for a retry, with the
% failing node in its blacklist. If a task fails too many times, as
% determined by check_failure_rate(), the whole job will be terminated.
-spec handle_data_error(task(), node()) -> _.
handle_data_error(Task, Host) ->
    {ok, MaxFail} = application:get_env(max_failure_rate),
    check_failure_rate(Task, MaxFail),
    spawn_link(fun() ->
        {A1, A2, A3} = now(),
        random:seed(A1, A2, A3),
        T = Task#task.taskblack,
        C = Task#task.fail_count + 1,
        S = lists:min([C * ?FAILED_MIN_PAUSE, ?FAILED_MAX_PAUSE]) +
                random:uniform(?FAILED_PAUSE_RANDOMIZE),
        event_server:event(
            Task#task.jobname,
            "~s:~B Task failed for the ~Bth time. "
                "Sleeping ~B seconds before retrying.",
            [Task#task.mode, Task#task.taskid, C, round(S / 1000)],
            []),
        timer:sleep(S),
        submit_task(Task#task{taskblack = [Host|T], fail_count = C})
    end).

-spec check_failure_rate(task(), non_neg_integer()) -> _.
check_failure_rate(Task, MaxFail) when Task#task.fail_count + 1 < MaxFail ->
    ok;
check_failure_rate(Task, MaxFail) ->
    Message = disco:format("Failed ~B times. At most ~B failures are allowed.",
                           [Task#task.fail_count + 1, MaxFail]),
    event_server:task_event(Task, {<<"ERROR">>, Message}),
    throw(logged_error).

-spec kill_job(nonempty_string(), nonempty_string(), [_], atom()) -> no_return().
kill_job(JobName, Msg, P, Type) ->
    event_server:event(JobName, Msg, P, []),
    disco_server:kill_job(JobName, 30000),
    gen_server:cast(event_server, {job_done, JobName}),
    exit(Type).

% run_task() is a common supervisor for both the map and reduce tasks.
% Its main function is to catch and report any errors that occur during
% work() calls.
-spec run_task([{non_neg_integer(), [{binary(), nonempty_string() | 'false'}]}],
    nonempty_string(), nonempty_string(), jobinfo()) -> [binary()].
run_task(Inputs, Mode, JobName, Job) ->
    case catch run_task_do(Inputs, Mode, JobName, Job) of
        {ok, Res} ->
            Res;
        logged_error ->
            kill_job(JobName,
                     "ERROR: Job terminated due to the previous errors",
                     [], logged_error);
        Error ->
            kill_job(JobName,
                     "ERROR: Job coordinator failed unexpectedly: ~p",
                     [Error], unknown_error)
    end.

run_task_do(Inputs, Mode, JobName, Job) ->
    {ok, Results} = work(Inputs, Mode, JobName, 0, Job, gb_trees:empty()),
    % if save=True, tasks output tag:// urls, not dir://.
    % We don't need to shuffle tags.
    Fun = fun ({_, <<"dir://", _/binary>>}) ->
                  true;
              (_) ->
                  false
          end,
    {DirUrls, Others} = lists:partition(Fun, gb_trees:values(Results)),
    {ok, Combined} = shuffle(JobName, Mode, DirUrls),
    {ok, lists:usort(([Url || {_Node, Url} <- Others])) ++ Combined}.

shuffle(_JobName, _Mode, []) ->
    {ok, []};
shuffle(JobName, Mode, DirUrls) ->
    event_server:event(JobName, "Shuffle phase starts", [], {}),
    T = now(),
    Ret = shuffle:combine_tasks(JobName, Mode, DirUrls),
    Elapsed = timer:now_diff(now(), T) div 1000,
    event_server:event(JobName, "Shuffle phase took ~bms.", [Elapsed], {}),
    Ret.

-spec map_input([binary() | [binary()]]) ->
    [{non_neg_integer(), [{binary(), nonempty_string() | 'false'}]}].
map_input(Inputs) ->
    Prefs = [map_input1(I) || I <- Inputs],
    lists:zip(lists:seq(0, length(Prefs) - 1), Prefs).

-spec map_input1(binary() | [binary()]) ->
    [{binary(), nonempty_string() | 'false'}].
map_input1(Inp) when is_list(Inp) ->
    [{X, pref_node(X)} || X <- Inp];
map_input1(Inp) ->
    [{Inp, pref_node(Inp)}].

-spec reduce_input(nonempty_string(), _, non_neg_integer()) ->
    [{non_neg_integer(), [{binary(), nonempty_string() | 'false'}]}].
reduce_input(JobName, Inputs, NRed) ->
    V = lists:any(fun erlang:is_list/1, Inputs),
    if V ->
            event_server:event(JobName,
                               "ERROR: Reduce doesn't support redundant inputs", [], []),
            throw({error, "redundant inputs in reduce"});
       true ->
            ok
    end,
    U = lists:usort([pref_node(X) || X <- Inputs]),
    N = length(U),
    D = dict:from_list(lists:zip(lists:seq(0, N - 1), U)),
    [{X, [{Inputs, dict:fetch(X rem N, D)}]} || X <- lists:seq(0, NRed - 1)].

% pref_node() suggests a preferred node for a task (one preserving locality)
% given the url of its input.

-spec pref_node(binary() | string()) -> 'false' | nonempty_string().
pref_node(Url) when is_binary(Url) -> pref_node(binary_to_list(Url));
pref_node(Url) ->
    case re:run(Url ++ "/", "[a-zA-Z0-9]+://(.*?)[/:]",
        [{capture, all_but_first}]) of
    nomatch -> false;
    {match, [{S, L}]} -> string:sub_string(Url, S + 1, S + L)
    end.

