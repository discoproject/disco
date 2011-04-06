-module(worker_runtime).
-export([init/2, handle/2, get_pid/1]).

-include("disco.hrl").

-record(state, {task :: task(),
                master :: node(),
                start_time :: timer:timestamp(),
                child_pid :: 'none' | non_neg_integer(),
                persisted_outputs :: [string()],
                output_filename :: 'none' | string(),
                output_file :: 'none' | file:io_device()}).

init(Task, Master) ->
    #state{task = Task,
           start_time = now(),
           master = Master,
           child_pid = none,
           persisted_outputs = [],
           output_filename = none,
           output_file = none}.

get_pid(#state{child_pid = Pid}) ->
    Pid.

handle({Type, Body}, S) ->
   %error_logger:info_report({"Got request", Type, Body}),
   case catch mochijson2:decode(Body) of
        {'EXIT', _} ->
            {error, {fatal,
                     ["Corrupted message: type '", Type, "', body:\n", Body]}};
        Json ->
           Ret = do_handle({Type, Json}, S),
           %error_logger:info_report({"Return", Ret}),
           Ret
    end.

do_handle({<<"PID">>, Pid}, S) ->
    {ok, {"OK", <<"ok">>}, S#state{child_pid = Pid}};

do_handle({<<"VSN">>, <<"1.0">>}, S) ->
    {ok, {"OK", <<"ok">>}, S};
do_handle({<<"VSN">>, Ver}, _S) ->
    {error, {fatal, ["Invalid worker version: ", Ver]}};

do_handle({<<"JOB">>, _Body}, #state{task = Task} = S) ->
    JobHome = disco_worker:jobhome(Task#task.jobname),
    {ok, {"JOB", list_to_binary(jobpack:jobfile(JobHome))}, S};

do_handle({<<"SET">>, _Body}, S) ->
    Port = list_to_integer(disco:get_setting("DISCO_PORT")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    Settings = {struct, [{<<"port">>, Port},
                         {<<"put_port">>, PutPort}]},
    {ok, {"SET", Settings}, S};

do_handle({<<"TSK">>, _Body}, #state{task = Task} = S) ->
    Master = disco:get_setting("DISCO_MASTER"),
    TaskInfo = {struct, [{<<"taskid">>, Task#task.taskid},
                         {<<"master">>, list_to_binary(Master)},
                         {<<"mode">>, list_to_binary(Task#task.mode)},
                         {<<"jobname">>, list_to_binary(Task#task.jobname)},
                         {<<"host">>, list_to_binary(disco:host(node()))}]},
    {ok, {"TSK", TaskInfo}, S};

do_handle({<<"STA">>, Msg}, #state{task = Task, master = Master} = S) ->
    disco_worker:event({<<"STA">>, Msg}, Task, Master),
    {ok, {"OK", <<"ok">>}, S};

do_handle({<<"INP">>, <<>>}, #state{task = Task} = S) ->
    Inputs = [[Id, Status, Urls] || {Id, Status, Urls} <- input(Task)],
    {ok, {"INP", [<<"done">>, Inputs]}, S};

do_handle({<<"INP">>, Id}, #state{task = Task} = S) ->
    case lists:keyfind(Id, 1, input(Task)) of
        {Id, Status, Urls} ->
            {ok, {"INP", [Status, Urls]}, S};
        false ->
            {ok, {"ERROR", [<<"No such input">>, Id]}, S}
    end;

do_handle({<<"DAT">>, Msg}, _S) ->
    {stop, {error, Msg}};

do_handle({<<"ERR">>, Msg}, _S) ->
    {stop, {fatal, Msg}};

do_handle({<<"OUT">>, Results}, S) ->
    case add_output(Results, S) of
        {ok, S1} ->
            {ok, {"OK", <<"ok">>}, S1};
        {error, Reason} ->
            {stop, {error, Reason}}
    end;

do_handle({<<"END">>, _Body}, #state{task = Task, master = Master} = S) ->
    case close_output(S) of
        ok ->
            Time = disco:format_time_since(S#state.start_time),
            Msg = ["Task finished in ", Time],
            disco_worker:event({<<"DONE">>, Msg}, Task, Master),
            {stop, {done, results(S)}};
        {error, Reason} ->
            {stop, {error, Reason}}
    end;

do_handle({Type, Body}, _S) ->
    {error, {fatal, ["Unknown message: type '", Type, "', body:\n", Body]}}.

input(Task) ->
    case Task#task.chosen_input of
        Binary when is_binary(Binary) ->
            [{1, <<"ok">>, [Binary]}];
        List when is_list(List) ->
            [{I, <<"ok">>, lists:flatten([Url])} || {I, Url} <- disco:enum(List)]
    end.

url_path(Task, Host, LocalFile) ->
    LocationPrefix = disco:joburl(Host, Task#task.jobname),
    filename:join(LocationPrefix, LocalFile).

local_results(Task, FileName) ->
    Host = disco:host(node()),
    Output = io_lib:format("dir://~s/~s",
                           [Host, url_path(Task, Host, FileName)]),
    list_to_binary(Output).

results(#state{output_filename = none, persisted_outputs = Outputs}) ->
    {none, Outputs};
results(#state{task = Task,
               output_filename = FileName,
               persisted_outputs = Outputs}) ->
    {local_results(Task, FileName), Outputs}.

-spec add_output(list(), #state{}) -> {ok, #state{}} | {error, string()}.
add_output([Tag, <<"tag">>], S) ->
    Result = list_to_binary(io_lib:format("tag://~s", [Tag])),
    Outputs = [Result | S#state.persisted_outputs],
    {ok, S#state{persisted_outputs = Outputs}};

add_output(RL, #state{task = Task, output_file = none} = S) ->
    ResultsFileName = results_filename(Task),
    Home = disco_worker:jobhome(Task#task.jobname),
    Path = filename:join(Home, ResultsFileName),
    ok = disco:ensure_dir(Path),
    case prim_file:open(Path, [write, raw]) of
        {ok, ResultsFile} ->
            add_output(RL, S#state{output_filename = ResultsFileName,
                                   output_file = ResultsFile});
        {error, Reason} ->
            {error, ioerror(["Opening index file at ", Path, " failed"], Reason)}
    end;

add_output(RL, #state{output_file = RF} = S) ->
    case prim_file:write(RF, format_output_line(S, RL)) of
        ok ->
            {ok, S};
        {error, Reason} ->
            {error, ioerror("Writing to index file failed", Reason)}
    end.

results_filename(Task) ->
    TimeStamp = timer:now_diff(now(), {0,0,0}),
    FileName = io_lib:format("~s-~B-~B.results", [Task#task.mode,
                                                  Task#task.taskid,
                                                  TimeStamp]),
    filename:join(".disco", FileName).

format_output_line(S, [LocalFile, Type]) ->
    format_output_line(S, [LocalFile, Type, <<"0">>]);
format_output_line(#state{task = Task}, [LocalFile, Type, Label]) ->
    Host = disco:host(node()),
    io_lib:format("~s ~s://~s/~s\n",
                  [Label, Type, Host, url_path(Task,
                                               Host,
                                               binary_to_list(LocalFile))]).


-spec close_output(#state{}) -> 'ok' | {error, string()}.
close_output(#state{output_file = none}) -> ok;
close_output(#state{output_file = File}) ->
    case {prim_file:sync(File), prim_file:close(File)} of
        {ok, ok} ->
            ok;
        {R1, R2} ->
            {error, Reason} = lists:max([R1, R2]),
            ioerror("Closing index file failed", Reason)
    end.

ioerror(Msg, Reason) ->
    [Msg, ": ", atom_to_list(Reason)].
