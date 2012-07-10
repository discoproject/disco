-module(worker_runtime).
-export([init/2, handle/2, get_pid/1]).

-include("common_types.hrl").
-include("disco.hrl").

-record(state, {task :: task(),
                inputs,
                master :: node(),
                start_time :: erlang:timestamp(),
                child_pid :: none | non_neg_integer(),
                persisted_outputs :: [string()],
                output_filename :: none | string(),
                output_file :: none | file:io_device()}).
-type state() :: #state{}.

-type results() :: {none | binary(), [binary()]}.

-export_type([state/0, results/0]).

-spec init(task(), node()) -> state().
init(Task, Master) ->
    #state{task = Task,
           inputs = worker_inputs:init(Task#task.chosen_input),
           start_time = now(),
           master = Master,
           child_pid = none,
           persisted_outputs = [],
           output_filename = none,
           output_file = none}.

-spec get_pid(state()) -> none | non_neg_integer().
get_pid(#state{child_pid = Pid}) ->
    Pid.

%-spec payload_type(binary()) -> json_validator:spec() | 'none'.
payload_type(<<"WORKER">>) -> {object, [{<<"version">>, string},
                                        {<<"pid">>, integer}]};
payload_type(<<"TASK">>) -> string;
payload_type(<<"MSG">>) -> string;
payload_type(<<"ERROR">>) -> string;
payload_type(<<"FATAL">>) -> string;
payload_type(<<"DONE">>) -> string;

payload_type(<<"INPUT">>) ->
    {opt, [{value, <<>>},
           {array, [{opt, [{value, <<"include">>},
                           {value, <<"exclude">>}]},
                    {hom_array, integer}]}
          ]};
payload_type(<<"INPUT_ERR">>) ->
    {array, [integer, {hom_array, integer}]};
payload_type(<<"OUTPUT">>) ->
    {opt, [{array, [string, string]},
           {array, [string, string, string]}]};

payload_type(_Type) -> none.

-type worker_msg() :: {nonempty_string(), term()}.

-type do_handle() :: {ok, worker_msg(), state()} | {ok, worker_msg(), state(), rate_limit}
                   | {error, {fatal, term()}, state()}
                   | {stop, {error | fatal | done, term()}}.
-type handle() :: do_handle() | {error, {fatal, term()}}.

-spec handle({binary(), binary()}, state()) -> handle().
handle({Type, Body}, S) ->
    Json = try mochijson2:decode(Body)
           catch _:_ -> invalid_json
           end,
    case {Json, payload_type(Type)} of
        {invalid_json, _} ->
            Err = ["Payload is not valid JSON: type '", Type, "', body:\n", Body],
            {error, {fatal, Err}};
        {_, none} ->
            Err = ["Unknown message type '", Type, "', body:\n", Body],
            {error, {fatal, Err}};
        {_, Spec} ->
            case json_validator:validate(Spec, Json) of
                ok ->
                    do_handle({Type, Json}, S);
                {error, E} ->
                    Msg = "Invalid message body (type '~s'): ~p",
                    {error, {fatal, io_lib:format(Msg, [Type, E])}}
            end
    end.

-spec do_handle({binary(), term()}, state()) -> do_handle().

do_handle({<<"WORKER">>, {struct, Worker}}, S) ->
    {_, Pid} = lists:keyfind(<<"pid">>, 1, Worker),
    S1 = S#state{child_pid = Pid},
    case lists:keyfind(<<"version">>, 1, Worker) of
        {_, <<"1.0">>} ->
            {ok, {"OK", <<"ok">>}, S1};
        {_, Ver} ->
            VerMsg = io_lib:format("~p", [Ver]),
            {error, {fatal, ["Invalid worker version: ", VerMsg]}, S1};
        _ ->
            {error, {fatal, ["No worker version received"]}, S1}
    end;

do_handle({<<"TASK">>, _Body}, #state{task = Task} = S) ->
    Master = disco:get_setting("DISCO_MASTER"),
    JobFile = jobpack:jobfile(disco_worker:jobhome(Task#task.jobname)),
    DiscoPort = list_to_integer(disco:get_setting("DISCO_PORT")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    DDFSData = disco:get_setting("DDFS_DATA"),
    DiscoData = disco:get_setting("DISCO_DATA"),
    TaskInfo = {struct, [{<<"taskid">>, Task#task.taskid},
                         {<<"master">>, list_to_binary(Master)},
                         {<<"disco_port">>, DiscoPort},
                         {<<"put_port">>, PutPort},
                         {<<"ddfs_data">>, list_to_binary(DDFSData)},
                         {<<"disco_data">>, list_to_binary(DiscoData)},
                         {<<"mode">>, list_to_binary(atom_to_list(Task#task.mode))},
                         {<<"jobfile">>, list_to_binary(JobFile)},
                         {<<"jobname">>, list_to_binary(Task#task.jobname)},
                         {<<"host">>, list_to_binary(disco:host(node()))}]},
    {ok, {"TASK", TaskInfo}, S};

do_handle({<<"MSG">>, Msg}, #state{task = Task, master = Master} = S) ->
    disco_worker:event({<<"MSG">>, Msg}, Task, Master),
    {ok, {"OK", <<"ok">>}, S, rate_limit};

do_handle({<<"INPUT">>, <<>>}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:all(Inputs)), S};

do_handle({<<"INPUT">>, [<<"include">>, Iids]}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:include(Iids, Inputs)), S};

do_handle({<<"INPUT">>, [<<"exclude">>, Iids]}, #state{inputs = Inputs} = S) ->
    {ok, input_reply(worker_inputs:exclude(Iids, Inputs)), S};

do_handle({<<"INPUT_ERR">>, [Iid, Rids]}, #state{inputs = Inputs} = S) ->
    Inputs1 = worker_inputs:fail(Iid, Rids, Inputs),
    [{_, Replicas} | _] = worker_inputs:include([Iid], Inputs1),
    R = gb_sets:from_list(Rids),
    case [E || [Rid, _Url] = E <- Replicas, not gb_sets:is_member(Rid, R)] of
        [] ->
            {ok, {"FAIL", <<>>}, S#state{inputs = Inputs1}};
        Valid ->
            {ok, {"RETRY", Valid}, S#state{inputs = Inputs1}}
    end;

do_handle({<<"ERROR">>, Msg}, _S) ->
    {stop, {error, Msg}};

do_handle({<<"FATAL">>, Msg}, _S) ->
    {stop, {fatal, Msg}};

do_handle({<<"OUTPUT">>, Results}, S) ->
    case add_output(Results, S) of
        {ok, S1} ->
            {ok, {"OK", <<"ok">>}, S1};
        {error, Reason} ->
            {stop, {error, Reason}}
    end;

do_handle({<<"PING">>, _Body}, S) ->
    {ok, {"OK", <<"pong">>}, S};

do_handle({<<"DONE">>, _Body}, #state{task = Task,
                                      master = Master,
                                      start_time = ST} = S) ->
    case close_output(S) of
        ok ->
            Time = disco:format_time_since(ST),
            Msg = ["Task finished in ", Time],
            disco_worker:event({<<"DONE">>, Msg}, Task, Master),
            {stop, {done, results(S)}};
        {error, Reason} ->
            {stop, {error, Reason}}
    end.

-spec input_reply([worker_inputs:worker_input()]) -> worker_msg().
input_reply(Inputs) ->
    {"INPUT", [<<"done">>, [[Iid, <<"ok">>, Repl] || {Iid, Repl} <- Inputs]]}.

-spec url_path(task(), host(), path()) -> file:filename().
url_path(Task, Host, LocalFile) ->
    LocationPrefix = disco:joburl(Host, Task#task.jobname),
    filename:join(LocationPrefix, LocalFile).

-spec local_results(task(), path()) -> binary().
local_results(Task, FileName) ->
    Host = disco:host(node()),
    Output = io_lib:format("dir://~s/~s",
                           [Host, url_path(Task, Host, FileName)]),
    list_to_binary(Output).

-spec results(state()) -> {none | binary(), [binary()]}.
results(#state{output_filename = none, persisted_outputs = Outputs}) ->
    {none, Outputs};
results(#state{task = Task,
               output_filename = FileName,
               persisted_outputs = Outputs}) ->
    {local_results(Task, FileName), Outputs}.

-spec add_output(list(), state()) -> {ok, state()} | {error, term()}.
add_output([Tag, <<"tag">>], #state{persisted_outputs = PO} = S) ->
    Result = list_to_binary(io_lib:format("tag://~s", [Tag])),
    {ok, S#state{persisted_outputs = [Result | PO]}};

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
            {error, ioerror("Opening index file at "++Path++" failed", Reason)}
    end;

add_output(RL, #state{output_file = RF} = S) ->
    case prim_file:write(RF, format_output_line(S, RL)) of
        ok ->
            {ok, S};
        {error, Reason} ->
            {error, ioerror("Writing to index file failed", Reason)}
    end.

-spec results_filename(task()) -> path().
results_filename(Task) ->
    TimeStamp = timer:now_diff(now(), {0,0,0}),
    FileName = io_lib:format("~s-~B-~B.results", [Task#task.mode,
                                                  Task#task.taskid,
                                                  TimeStamp]),
    filename:join(".disco", FileName).

-spec format_output_line(state(), [string()|binary()]) -> iolist().
format_output_line(S, [LocalFile, Type]) ->
    format_output_line(S, [LocalFile, Type, <<"0">>]);
format_output_line(#state{task = Task}, [LocalFile, Type, Label]) ->
    Host = disco:host(node()),
    io_lib:format("~s ~s://~s/~s\n",
                  [Label, Type, Host, url_path(Task,
                                               Host,
                                               binary_to_list(LocalFile))]).


-spec close_output(state()) -> ok | {error, term()}.
close_output(#state{output_file = none}) -> ok;
close_output(#state{output_file = File}) ->
    case {prim_file:sync(File), prim_file:close(File)} of
        {ok, ok} ->
            ok;
        {R1, R2} ->
            {error, Reason} = lists:max([R1, R2]),
            {error, ioerror("Closing index file failed", Reason)}
    end.

-spec ioerror(nonempty_string(), atom()) -> nonempty_string().
ioerror(Msg, Reason) ->
    Msg ++ ": " ++ atom_to_list(Reason).
