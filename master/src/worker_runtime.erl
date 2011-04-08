-module(worker_runtime).
-export([init/2, handle/2, get_pid/1]).

-include("disco.hrl").

-record(state, {task :: task(),
                inputs,
                master :: node(),
                start_time :: timer:timestamp(),
                child_pid :: 'none' | non_neg_integer(),
                persisted_outputs :: [string()],
                output_filename :: 'none' | string(),
                output_file :: 'none' | file:io_device()}).

init(Task, Master) ->
    #state{task = Task,
           inputs = worker_inputs:init(Task#task.chosen_input),
           start_time = now(),
           master = Master,
           child_pid = none,
           persisted_outputs = [],
           output_filename = none,
           output_file = none}.

get_pid(#state{child_pid = Pid}) ->
    Pid.

payload_type(<<"PID">>) -> integer;
payload_type(<<"VSN">>) -> string;
payload_type(<<"JOB">>) -> string;
payload_type(<<"SET">>) -> string;
payload_type(<<"TSK">>) -> string;
payload_type(<<"STA">>) -> string;
payload_type(<<"DAT">>) -> string;
payload_type(<<"ERR">>) -> string;
payload_type(<<"END">>) -> string;

payload_type(<<"INP">>) ->
    {array, [{opt, [{value, <<"include">>},
                    {value, <<"exclude">>}]},
             {hom_array, integer}]};
payload_type(<<"EREP">>) ->
    {array, [integer, {hom_array, integer}]};
payload_type(<<"OUT">>) ->
    {opt, [{array, [string, string]},
           {array, [string, string, string]}]};

payload_type(_Type) -> none.


handle({Type, Body}, S) ->
   %error_logger:info_report({"Got request", Type, Body}),
   case catch mochijson2:decode(Body) of
        {'EXIT', _} ->
            {error, {fatal,
                     ["Corrupted message: type '", Type, "', body:\n", Body]}};
        Json ->
           %error_logger:info_report({"worker: handling", Type, Json}),
           case payload_type(Type) of
               none -> ok;
               Spec -> case json_validator:validate(Spec, Json) of
                           ok -> ok;
                           E -> error_logger:error_report({"invalid payload type", Type, E})
                       end
           end,
           Ret = do_handle({Type, Json}, S),
           %error_logger:info_report({"Return", Ret}),
           Ret
    end.

do_handle({<<"PID">>, Pid}, S) ->
    {ok, {"OK", <<"ok">>}, S#state{child_pid = Pid}};

do_handle({<<"VSN">>, <<"1.0">>}, S) ->
    {ok, {"OK", <<"ok">>}, S};
do_handle({<<"VSN">>, Ver}, _S) ->
    {error, {fatal, ["Invalid worker version: ", io_lib:format("~p", [Ver])]}};

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

do_handle({<<"INP">>, <<>>}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:all(Inputs)), S};

do_handle({<<"INP">>, [<<"include">>, Iids]}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:include(Iids, Inputs)), S};

do_handle({<<"INP">>, [<<"exclude">>, Iids]}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:exclude(Iids, Inputs)), S};

do_handle({<<"INP">>, _}, _S) ->
    {stop, {fatal, "Invalid INP request"}};

do_handle({<<"EREP">>, [Iid, Rids]}, #state{inputs = Inputs} = S) ->
    Inputs1 = worker_inputs:fail(Iid, Rids, Inputs),
    {_, Replicas} = worker_inputs:include([Iid], Inputs1),
    R = gb_sets:from_list(Rids),
    case [E || [Rid, _Url] = E <- Replicas, not gb_sets:is_member(Rid, R)] of
        [] ->
            {ok, {"FAIL", <<>>}, S#state{inputs = Inputs1}};
        Valid ->
            {ok, {"RETRY", Valid}, S#state{inputs = Inputs1}}
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

do_handle({Type, _Body}, _S) ->
    {error, {fatal, ["Unknown message type:", Type]}}.

input_reply(Inputs) ->
    {"INP", [<<"done">>, [[Iid, <<"ok">>, Repl] || {Iid, Repl} <- Inputs]]}.

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
