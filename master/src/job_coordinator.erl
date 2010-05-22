
-module(job_coordinator).
-export([new/1]).

-include("task.hrl").

-define(TASK_MAX_FAILURES, 100).
-define(FAILED_TASK_PAUSE, 1000).

% In theory we could keep the HTTP connection pending until the job
% finishes but in practice long-living HTTP connections are a bad idea.
% Thus, the HTTP request spawns a new process, job_coordinator, that
% takes care of coordinating the whole map-reduce show, including
% fault-tolerance. The HTTP request returns immediately. It may poll
% the job status e.g. by using handle_ctrl's get_results.
new(PostData) ->
    TMsg = "couldn't start a new job coordinator in 10s (master busy?)",
    S = self(),
    P = spawn(fun() -> init_job_coordinator(S, PostData) end),
    receive
    {P, ok, JobName} -> {ok, JobName};
    {P, env_failed} -> throw("creating job directories failed "
             "(disk full?)");
    {P, invalid_prefix} -> throw("invalid prefix");
    {P, invalid_jobdesc} -> throw("invalid job description");
    {P, timeout} -> throw(TMsg);
    _ -> throw("job coordinator failed")
    after 10000 ->
    throw(TMsg)
    end.

% job_coordinator() orchestrates map/reduce tasks for a job
init_job_coordinator(Parent, PostData) ->
    Msg = netstring:decode_netstring_fd(PostData),
    case catch find_values(Msg) of
    {'EXIT', _} ->
        Parent ! {self(), invalid_jobdesc};
    Params ->
        init_job_coordinator(Parent, Params, PostData)
    end.

init_job_coordinator(Parent, {Prefix, JobInfo}, PostData) ->
    C = string:chr(Prefix, $/) + string:chr(Prefix, $.),
    if C > 0 ->
    Parent ! {self(), invalid_prefix};
    true ->
    case gen_server:call(event_server, {new_job, Prefix, self()}, 10000) of
        {ok, JobName} ->
        save_params(JobName, PostData),
        Parent ! {self(), ok, JobName},
        job_coordinator(JobName, JobInfo);
        _ ->
        Parent ! {self(), timeout}
    end
    end.

save_params(Name, PostData) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    Home = disco_server:jobhome(Name),
    ok = file:write_file(filename:join([Root, Home, "params"]), PostData).

field_exists(Msg, Opt) ->
    lists:keysearch(Opt, 1, Msg) =/= false.

find_values(Msg) ->

    {value, {_, PrefixBinary}} = lists:keysearch(<<"prefix">>, 1, Msg),
    Prefix = binary_to_list(PrefixBinary),

    {value, {_, InputStr}} = lists:keysearch(<<"input">>, 1, Msg),
    Inputs = [case string:tokens(Inp, "\n") of
		      [X] -> list_to_binary(X);
		      Y -> [list_to_binary(X) || X <- Y]
		  end || Inp <- string:tokens(binary_to_list(InputStr), " ")],

    {value, {_, MaxCStr}} = lists:keysearch(<<"sched_max_cores">>, 1, Msg),
    MaxCores = list_to_integer(binary_to_list(MaxCStr)),

    {value, {_, NRedStr}} = lists:keysearch(<<"nr_reduces">>, 1, Msg),
    NumRed = list_to_integer(binary_to_list(NRedStr)),

    {Prefix, #jobinfo{
       nr_reduce = NumRed,
       inputs = Inputs,
       max_cores = MaxCores,
       map = field_exists(Msg, <<"map">>),
       reduce = field_exists(Msg, <<"reduce">>),
       force_local = field_exists(Msg, <<"sched_force_local">>),
       force_remote = field_exists(Msg, <<"sched_force_remote">>)
    }}.


% work() is the heart of the map/reduce show. First it distributes tasks
% to nodes. After that, it starts to wait for the results and finally
% returns when it has gathered all the results.

%. 1. Basic case: Tasks to distribute, maximum number of concurrent tasks (N)
%  not reached.
work([{TaskID, Input}|Inputs], Mode, Name, N, Job, Res)
    when N < Job#jobinfo.max_cores ->

    Task = #task{jobname = Name,
         taskid = TaskID,
         mode = Mode,
         taskblack = [],
         input = Input,
         from = self(),
         force_local = Job#jobinfo.force_local,
         force_remote = Job#jobinfo.force_remote
    },
    submit_task(Task),
    work(Inputs, Mode, Name, N + 1, Job, Res);

% 2. Tasks to distribute but the maximum number of tasks are already running.
% Wait for tasks to return. Note that wait_workers() may return with the same
% number of tasks still running, i.e. N = M.
work([_|_] = IArg, Mode, Name, N, Job, Res) when N >= Job#jobinfo.max_cores ->
    {M, NRes} = wait_workers(N, Res, Name, Mode),
    work(IArg, Mode, Name, M, Job, NRes);

% 3. No more tasks to distribute. Wait for tasks to return.
work([], Mode, Name, N, Job, Res) when N > 0 ->
    {M, NRes} = wait_workers(N, Res, Name, Mode),
    work([], Mode, Name, M, Job, NRes);

% 4. No more tasks to distribute, no more tasks running. Done.
work([], _Mode, _Name, 0, _Job, Res) -> {ok, Res}.

% wait_workers receives messages from disco_server:clean_worker() that is
% called when a worker exits.

% Error condition: should not happen.
wait_workers(0, _Res, _Name, _Mode) ->
    throw("Nothing to wait");

wait_workers(N, Results, Name, Mode) ->
    receive
    {{job_ok, Result}, Task, Node} ->
        event_server:event(Name,
        "Received results from ~s:~B @ ~s.",
            [Task#task.mode, Task#task.taskid, Node],
            {task_ready, Mode}),
        % We may get a multiple instances of the same result
        % address but only one of them must be taken into
        % account. That's why we use gb_trees instead of a list.
        {N - 1, gb_trees:enter(Result, true, Results)};

    {{data_error, _Msg}, Task, Node} ->
        handle_data_error(Task, Node),
        {N, Results};

    {{job_error, _Error}, _Task, _Node} ->
        throw(logged_error);

    {{error, Error}, Task, Node} ->
        event_server:event(Name,
        "ERROR: Worker crashed in ~s:~B @ ~s: ~p",
            [Task#task.mode, Task#task.taskid,
            Node, Error], []),
        throw(logged_error);

    Error ->
        event_server:event(Name,
        "ERROR: Received an unknown error: ~p",
            [Error], []),
        throw(logged_error)
    end.

submit_task(Task) ->
    case catch gen_server:call(disco_server, {new_task, Task}, 30000) of
    ok -> ok;
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
handle_data_error(Task, Node) ->
    MaxFail = case application:get_env(max_failure_rate) of
    undefined -> ?TASK_MAX_FAILURES;
    {ok, N0} -> N0
    end,
    check_failure_rate(Task, MaxFail),

    Inputs = Task#task.input,
    NInputs =
	    case Inputs of
		[_, _ | _] ->
		    [{X, N} || {X, N} <- Inputs, X =/= Task#task.chosen_input];
		_ ->
		    Inputs
	    end,
    spawn_link(fun() ->
        T = Task#task.taskblack,
        timer:sleep(length(T) * ?FAILED_TASK_PAUSE),
        submit_task(Task#task{taskblack = [Node|T], input = NInputs})
    end).

check_failure_rate(Task, MaxFail)
    when length(Task#task.taskblack) + 1 =< MaxFail -> ok;
check_failure_rate(Task, MaxFail) ->
    event_server:event(Task#task.jobname,
    "ERROR: ~s:~B failed ~B times. At most ~B failures "
    "are allowed. Aborting job.", 
        [Task#task.mode,
         Task#task.taskid,
         length(Task#task.taskblack) + 1,
         MaxFail], []),
    throw(logged_error).

kill_job(Name, Msg, P, Type) ->
    event_server:event(Name, Msg, P, []),
    gen_server:call(disco_server, {kill_job, Name}, 30000),
    gen_server:cast(event_server, {job_done, Name}),
    exit(Type).

% run_task() is a common supervisor for both the map and reduce tasks.
% Its main function is to catch and report any errors that occur during
% work() calls.
run_task(Inputs, Mode, Name, Job) ->
    Results = case catch work(Inputs, Mode, Name,
        0, Job, gb_trees:empty()) of
    {ok, Res} -> Res;
    logged_error ->
        kill_job(Name,
        "ERROR: Job terminated due to the previous errors",
        [], logged_error);
    Error ->
        kill_job(Name,
        "ERROR: Job coordinator failed unexpectedly: ~p",
        [Error], unknown_error)
    end,
    [list_to_binary(X) || X <- gb_trees:keys(Results)].

job_coordinator(Name, Job) ->
    Started = now(),
    event_server:event(Name, "Starting job", [], {job_data, Job}),

    case catch gen_server:call(disco_server, {new_job, Name, self()}, 30000) of
    ok -> ok;
    R ->
        event_server:event(Name,
        "ERROR: Job initialization failed: ~p", [R], {}),
        exit(job_init_failed)
    end,

    RedInputs = if Job#jobinfo.map ->
        event_server:event(Name, "Map phase", [], {}),
        MapResults = run_task(map_input(Job#jobinfo.inputs),
            "map", Name, Job),
        event_server:event(Name, "Map phase done", [], {map_ready, MapResults}),
        MapResults;
    true ->
        Job#jobinfo.inputs
    end,

    if Job#jobinfo.reduce ->
        event_server:event(Name, "Starting reduce phase", [], {}),
        RedResults = run_task(reduce_input(
            Name, RedInputs, Job#jobinfo.nr_reduce),
            "reduce", Name,
            Job#jobinfo{force_local = false, force_remote = false}),

        event_server:event(Name, "Reduce phase done", [], []),
        event_server:event(Name, "READY: Job finished in " ++
            disco_server:format_time(Started),
            [], {ready, RedResults});
    true ->
        event_server:event(Name, "READY: Job finished in " ++
            disco_server:format_time(Started),
            [], {ready, RedInputs})
    end,
    gen_server:cast(event_server, {job_done, Name}).

map_input(Inputs) ->
    Prefs = [map_input1(I) || I <- Inputs],
    lists:zip(lists:seq(0, length(Prefs) - 1), Prefs).

map_input1(Inp) when is_list(Inp) ->
    [{<<"'", X/binary, "' ">>, pref_node(X)} || X <- Inp];
map_input1(Inp) ->
    [{<<"'", Inp/binary, "' ">>, pref_node(Inp)}].

reduce_input(Name, Inputs, NRed) ->
    V = lists:any(fun erlang:is_list/1, Inputs),
    if V ->
    event_server:event(Name,
        "ERROR: Reduce doesn't support redundant inputs", [], []),
    throw({error, "redundant inputs in reduce"});
    true -> ok
    end,
    B = << <<"'", X/binary, "' ">> || X <- Inputs >>,
    U = lists:usort([pref_node(X) || X <- Inputs]),
    N = length(U),
    D = dict:from_list(lists:zip(lists:seq(0, N - 1), U)),
    [{X, [{B, dict:fetch(X rem N, D)}]} || X <- lists:seq(0, NRed - 1)].

% pref_node() suggests a preferred node for a task (one preserving locality)
% given the url of its input.

pref_node(Url) when is_binary(Url) -> pref_node(binary_to_list(Url));
pref_node(Url) ->
    case re:run(Url ++ "/", "[a-zA-Z0-9]+://(.*?)[/:]",
        [{capture, all_but_first}]) of
    nomatch -> false;
    {match, [{S, L}]} -> string:sub_string(Url, S + 1, S + L)
    end.
