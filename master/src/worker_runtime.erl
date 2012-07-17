-module(worker_runtime).
-export([init/2, handle/2, get_pid/1]).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-type remote_output() :: {label(), data_size(), [data_replica()]}.
-record(state, {jobname    :: jobname(),
                task       :: task(),
                master     :: node(),
                host       :: host(),
                inputs     :: worker_inputs:state(),
                start_time :: erlang:timestamp(),

                child_pid       = none             :: none | non_neg_integer(),
                failed_inputs   = gb_sets:empty()  :: gb_set(), % [seq_id()]
                remote_outputs  = []               :: [remote_output()],
                local_labels    = gb_trees:empty() :: gb_tree(),
                output_filename = none             :: none | string(),
                output_file     = none             :: none | file:io_device()}).
-type state() :: #state{}.

-export_type([state/0]).

-spec init(task(), node()) -> state().
init({#task_spec{jobname = JN, grouping = Grouping, group = Group},
      #task_run{input = Inputs}} = Task, Master) ->
    #state{jobname = JN,
           task    = Task,
           master  = Master,
           host    = disco:host(node()),
           inputs  = worker_inputs:init(Inputs, Grouping, Group),
           start_time = now()}.

-spec get_pid(state()) -> none | non_neg_integer().
get_pid(#state{child_pid = Pid}) ->
    Pid.

% Input message format.

-spec payload_type(binary()) -> json_validator:spec() | none.
payload_type(<<"WORKER">>) -> {object, [{<<"version">>, string},
                                        {<<"pid">>, integer}]};
payload_type(<<"TASK">>)  -> string;
payload_type(<<"MSG">>)   -> string;
payload_type(<<"ERROR">>) -> string;
payload_type(<<"FATAL">>) -> string;
payload_type(<<"DONE">>)  -> string;
payload_type(<<"INPUT">>) -> {opt, [{value, <<>>},
                                    {array, [{opt, [{value, <<"include">>},
                                                    {value, <<"exclude">>}]},
                                             {hom_array, integer}]}]};
payload_type(<<"INPUT_ERR">>) -> {array, [integer, {hom_array, integer}]};
payload_type(<<"OUTPUT">>)    -> {array, [integer, string, integer]};
payload_type(_Type) -> none.

% Core protocol handling.

-type worker_msg() :: {nonempty_string(), term()}.
-type output_msg() :: {label(), binary(), data_size()}.

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
        {_, <<"1.1">>} ->
            {ok, {"OK", <<"ok">>}, S1};
        {_, Ver} ->
            VerMsg = io_lib:format("~p", [Ver]),
            {error, {fatal, ["Invalid worker version: ", VerMsg]}, S1};
        _ ->
            {error, {fatal, ["No worker version received"]}, S1}
    end;

do_handle({<<"TASK">>, _Body}, #state{host = Host, task = {TS, _TR}} = S) ->
    #task_spec{jobname = JN, taskid = TaskId, stage = Stage, group = G} = TS,
    Master = disco:get_setting("DISCO_MASTER"),
    JobFile = jobpack:jobfile(disco_worker:jobhome(JN)),
    DiscoPort = list_to_integer(disco:get_setting("DISCO_PORT")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    DDFSData = disco:get_setting("DDFS_DATA"),
    DiscoData = disco:get_setting("DISCO_DATA"),
    TaskInfo = {struct, [{<<"taskid">>, TaskId},
                         {<<"master">>, list_to_binary(Master)},
                         {<<"disco_port">>, DiscoPort},
                         {<<"put_port">>, PutPort},
                         {<<"ddfs_data">>, list_to_binary(DDFSData)},
                         {<<"disco_data">>, list_to_binary(DiscoData)},
                         {<<"stage">>, Stage},
                         {<<"group">>, group_to_json(G)},
                         {<<"jobfile">>, list_to_binary(JobFile)},
                         {<<"jobname">>, list_to_binary(JN)},
                         {<<"host">>, list_to_binary(Host)}]},
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

do_handle({<<"INPUT_ERR">>, [Iid, Rids]}, #state{inputs = Inputs,
                                                 failed_inputs = Failed} = S) ->
    Inputs1 = worker_inputs:fail(Iid, Rids, Inputs),
    [{_, Replicas} | _] = worker_inputs:include([Iid], Inputs1),
    R = gb_sets:from_list(Rids),
    case [E || [Rid, _Url] = E <- Replicas, not gb_sets:is_member(Rid, R)] of
        [] ->
            Failed1 = gb_sets:add(Iid, Failed),
            {ok, {"FAIL", <<>>}, S#state{inputs = Inputs1,
                                         failed_inputs = Failed1}};
        Valid ->
            {ok, {"RETRY", Valid}, S#state{inputs = Inputs1}}
    end;

do_handle({<<"ERROR">>, Msg}, _S) ->
    {stop, {error, Msg}};

do_handle({<<"FATAL">>, Msg}, _S) ->
    {stop, {fatal, Msg}};

do_handle({<<"OUTPUT">>, Output}, S) ->
    case add_output(list_to_tuple(Output), S) of
        {ok, S1}   -> {ok, {"OK", <<"ok">>}, S1};
        {error, E} -> {stop, {error, E}}
    end;

do_handle({<<"PING">>, _Body}, S) ->
    {ok, {"OK", <<"pong">>}, S};

do_handle({<<"DONE">>, _Body}, #state{task = Task,
                                      master = Master,
                                      start_time = ST} = S) ->
    case close_output(S) of
        ok ->
            Msg = ["Task finished in ", disco:format_time_since(ST)],
            disco_worker:event({<<"DONE">>, Msg}, Task, Master),
            {stop, {done, results(S)}};
        {error, _Reason} = E ->
            {stop, E}
    end.

% Input utilities.
-spec input_reply([worker_inputs:worker_input()]) -> worker_msg().
input_reply(Inputs) ->
    {"INPUT", [<<"done">>, [[Iid, <<"ok">>, Repl] || {Iid, Repl} <- Inputs]]}.

% Output utilities.

-spec group_to_json(group()) -> list().
group_to_json({L, H}) when is_atom(H) ->
    [L, list_to_binary(atom_to_list(H))];
group_to_json({L, H}) when is_list(H) ->
    [L, list_to_binary(H)].

-spec ioerror(nonempty_string(), atom()) -> nonempty_string().
ioerror(Msg, Reason) ->
    Msg ++ ": " ++ atom_to_list(Reason).

-spec url_path(jobname(), host(), path()) -> file:filename().
url_path(JobName, Host, LocalFile) ->
    LocationPrefix = disco:joburl(Host, JobName),
    filename:join(LocationPrefix, LocalFile).

-spec local_results_filename(task()) -> path().
local_results_filename({#task_spec{stage = S, taskid = TaskId}, #task_run{}}) ->
    TimeStamp = timer:now_diff(now(), {0,0,0}),
    FileName = io_lib:format("~s-~B-~B.results", [S, TaskId, TimeStamp]),
    filename:join(".disco", FileName).

-spec output_type(url()) -> local | remote | invalid.
output_type(Url) ->
    U = binary_to_list(Url),
    {S, H, Path, _Q, _F} = mochiweb_util:urlsplit(U),
    case {S, H, Path} of
        {"", "", "/" ++ _P} -> invalid;  % Forbid absolute paths
        {"", "", _P}        -> local;
        _                   -> remote
    end.

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

% Handling recording of worker outputs.

-spec add_output(output_msg(), state()) -> {ok, state()} | {error, term()}.
add_output({L, Url, Size} = O, #state{remote_outputs = RO} = S) ->
    case output_type(Url) of
        invalid ->
            {error, {invalid_output, Url}};
        remote ->
            DataOutput = {L, Size, [{Url, disco:preferred_host(Url)}]},
            {ok, S#state{remote_outputs = [DataOutput | RO]}};
        local ->
            local_output(O, S)
    end.

-spec local_output(output_msg(), state()) -> {ok, state()} | {error, term()}.
local_output(O, #state{jobname = JN, task = Task, output_file = none} = S) ->
    FileName = local_results_filename(Task),
    Home = disco_worker:jobhome(JN),
    Path = filename:join(Home, FileName),
    ok = disco:ensure_dir(Path),
    case prim_file:open(Path, [write, raw]) of
        {ok, ResultsFile} ->
            local_output(O, S#state{output_filename = FileName,
                                    output_file     = ResultsFile});
        {error, E} ->
            {error, ioerror("Opening index file at " ++ Path ++ " failed", E)}
    end;
local_output({L, _U, Sz} = O, #state{output_file = RF,
                                     local_labels = Labels} = S) ->
    case prim_file:write(RF, format_local_output(O, S)) of
        ok ->
            CurSz = case gb_trees:lookup(L, Labels) of
                        none -> 0;
                        {value, LSize} -> LSize
                    end,
            {ok, S#state{local_labels = gb_trees:enter(L, CurSz + Sz, Labels)}};
        {error, E} ->
            {error, ioerror("Writing to index file failed", E)}
    end.

-spec format_local_output(output_msg(), state()) -> iolist().
format_local_output({L, LocalFile, Size}, #state{jobname = JN, host = Host}) ->
    Url = url_path(JN, Host, binary_to_list(LocalFile)),
    io_lib:format("~B disco://~s/~s ~B\n", [L, Host, Url, Size]).

% Convert recorded outputs into pipeline format.

-spec local_results(jobname(), host(), path(), gb_tree()) -> dir_spec().
local_results(JobName, Host, FileName, Labels) ->
    UPath = url_path(JobName, Host, FileName),
    Dir = erlang:iolist_to_binary(io_lib:format("dir://~s/~s", [Host, UPath])),
    {dir, {Host, Dir, gb_trees:to_list(Labels)}}.

-spec results(state()) -> [task_output()].
results(#state{output_filename = none, remote_outputs = ROutputs}) ->
    disco:enum([{data, RO} || RO <- ROutputs]);
results(#state{jobname = JobName,
               host = Host,
               output_filename = FileName,
               local_labels = Labels,
               remote_outputs = ROutputs}) ->
    disco:enum([local_results(JobName, Host, FileName, Labels)
                | [{data, RO} || RO <- ROutputs]]).
