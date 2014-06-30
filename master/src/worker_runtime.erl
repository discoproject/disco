-module(worker_runtime).
-export([init/2, handle/2, get_pid/1, add_inputs/2]).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").
-include("config.hrl").
-include("ddfs.hrl").

-define(PUT_TIMEOUT, 10 * ?MINUTE).
-define(MAX_SAVE_ATTEMPTS, 10).

-type remote_output() :: {label(), data_size(), [data_replica()]}.

-record(state, {jobname      :: jobname(),
                task         :: task(),
                master       :: node(),
                host         :: host(),
                inputs       :: worker_inputs:state(),
                start_time   :: erlang:timestamp(),
                save_outputs :: boolean(),
                save_info    :: string(),

                input_requested = false            :: boolean(),
                child_pid       = none             :: none | non_neg_integer(),
                failed_inputs   = gb_sets:empty()  :: disco_gbset(seq_id()),
                remote_outputs  = []               :: [remote_output()],
                local_labels    = gb_trees:empty() :: disco_gbtree(label(), data_size()),
                output_filename = none             :: none | path(),
                output_file     = none             :: none | file:io_device()}).
-type state() :: #state{}.

-export_type([state/0]).

-spec init(task(), node()) -> state().
init({#task_spec{jobname = JN, grouping = Grouping, group = Group,
                 all_inputs = AllInputs,
                 save_outputs = Save, save_info = SaveInfo},
      #task_run{input = Inputs}} = Task, Master) ->
    #state{jobname = JN,
           task    = Task,
           master  = Master,
           host    = disco:host(node()),
           inputs  = worker_inputs:init(Inputs, Grouping, Group, AllInputs),
           start_time   = now(),
           save_outputs = Save,
           save_info = SaveInfo}.

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
                   | {ok, noreply, state()}
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
    #task_spec{jobname = JN, taskid = TaskId, stage = Stage,
               group = G, grouping = Gg} = TS,
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
                         {<<"grouping">>, grouping_to_json(Gg)},
                         {<<"group">>, group_to_json(G)},
                         {<<"jobfile">>, list_to_binary(JobFile)},
                         {<<"jobname">>, list_to_binary(JN)},
                         {<<"host">>, list_to_binary(Host)}]},
    {ok, {"TASK", TaskInfo}, S};

do_handle({<<"MSG">>, Msg}, #state{task = Task, master = Master} = S) ->
    disco_worker:event({<<"MSG">>, Msg}, Task, Master),
    {ok, {"OK", <<"ok">>}, S, rate_limit};

do_handle({<<"INPUT">>, <<>>}, #state{inputs = InputState} = S) ->
    produce_inputs(fun worker_inputs:all/1, InputState, S);

do_handle({<<"INPUT">>, [<<"include">>, Iids]}, #state{inputs = InputState} = S) ->
    produce_inputs({fun worker_inputs:include/2, Iids}, InputState, S);

do_handle({<<"INPUT">>, [<<"exclude">>, Iids]}, #state{inputs = InputState} = S) ->
    produce_inputs({fun worker_inputs:exclude/2, Iids}, InputState, S);

do_handle({<<"INPUT_ERR">>, [Iid, Rids]}, #state{inputs = Inputs,
                                                 failed_inputs = Failed} = S) ->
    Inputs1 = worker_inputs:fail(Iid, Rids, Inputs),
    [{_, _L, Replicas} | _] = worker_inputs:include([Iid], Inputs1),
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
            case results(S) of
                {ok, R} -> {stop, {done, R}};
                E       -> {stop, E}
            end;
        {error, _Reason} = E ->
            {stop, E}
    end.

% Input utilities.
-spec input_reply([worker_inputs:worker_input()], boolean()) -> worker_msg().
input_reply(Inputs, Done) ->
    Status = case Done of
        false -> <<"more">>;
        true  -> <<"done">>
    end,
    {"INPUT", [Status, [[Iid, <<"ok">>, L, Repl]
                            || {Iid, L, Repl} <- Inputs]]}.

get_inputs(Function, InputState) ->
    case Function of
        {F, I} -> F(I, InputState);
        F -> F(InputState)
    end.

-spec add_inputs(done | [{input_id(), data_input()}], state()) ->
    {_, state()}.
add_inputs(Inputs, #state{input_requested = Requested, inputs = InputState}=S) ->
    InputState1 = worker_inputs:add_inputs(Inputs, InputState),
    Reply = case Requested of
        false -> none;
        true  ->
            case Inputs of
                done -> <<"done">>;
                _    -> <<"more">>
            end
    end,
    {Reply, S#state{input_requested = false, inputs = InputState1}}.

produce_inputs(Function, InputState, S) ->
    Inputs = get_inputs(Function, InputState),
    case {worker_inputs:is_input_done(InputState), length(Inputs)} of
        {false, 0} ->
            {ok, noreply, S#state{input_requested = true}};
        {Done, _} ->
            {ok, input_reply(Inputs, Done), S}
    end.

% Output utilities.

-spec group_to_json(group()) -> list().
group_to_json({L, none}) ->
    [L, <<"">>];
group_to_json({L, H}) when is_list(H) ->
    [L, list_to_binary(H)].

-spec grouping_to_json(label_grouping()) -> binary().
grouping_to_json(Gg) ->
    list_to_binary(atom_to_list(Gg)).

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

-spec local_results(jobname(), host(), path(), disco_gbtree(label(), data_size())) -> dir_spec().
local_results(JobName, Host, FileName, Labels) ->
    UPath = url_path(JobName, Host, FileName),
    Dir = erlang:iolist_to_binary(io_lib:format("dir://~s/~s", [Host, UPath])),
    {dir, {Host, Dir, gb_trees:to_list(Labels)}}.

-spec results(state()) -> {ok, [task_output()]} | {error, term()}.
results(#state{output_filename = none, remote_outputs = ROutputs}) ->
    {ok, disco:enum([{data, RO} || RO <- ROutputs])};
results(#state{jobname = JobName,
               host = Host,
               save_outputs = false,
               output_filename = FileName,
               local_labels = Labels,
               remote_outputs = ROutputs}) ->
    {ok, disco:enum([local_results(JobName, Host, FileName, Labels)
                     | [{data, RO} || RO <- ROutputs]])};
results(#state{jobname = JobName,
               task = Task,
               master = Master,
               save_outputs = true,
               save_info = SaveInfo,
               output_filename = FileName,
               remote_outputs = ROutputs}) ->
    case save_locals_to_dfs(JobName, FileName, Master, Task, SaveInfo) of
        {ok, Locs} -> {ok, disco:enum([{data, O} || O <- ROutputs ++ Locs])};
        E          -> E
    end.

-spec save_locals_to_dfs(jobname(), path(), node(), task(), string()) ->
                                 {ok, [remote_output()]} | {error, term()}.
save_locals_to_dfs(JN, FileName, Master, Task, SaveInfo) ->
    IndexFile = filename:join(disco_worker:jobhome(JN), FileName),
    NReplicas = list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS")),
    {#task_spec{taskid = TaskId}, #task_run{runid = RunId}} = Task,
    case prim_file:read_file(IndexFile) of
        {ok, Index} ->
            Locals =
                try {ok, parse_index(Index)}
                catch
                    Err ->
                        error_logger:warning_msg("Error parsing ~s: ~p",
                                                 [IndexFile, Err]),
                        {error, Err};
                    K:V ->
                        error_logger:warning_msg("Error parsing ~s: ~p:~p",
                                                 [IndexFile, K, V]),
                        {error, bad_index_file}
                end,
            case Locals of
                {ok, L} ->
                        case lists:prefix("hdfs", SaveInfo) of
                            true  -> save_hdfs(JN, L, SaveInfo, TaskId, 0, []);
                            false -> save_locals(L, NReplicas, Master, JN, TaskId, RunId, [])
                        end;
                {error, _} = E1 -> E1
            end;
        {error, _} = E2 ->
            E2
    end.

-spec save_hdfs(jobname(), [{label(), url(), data_size()}], string(), task_id(),
                non_neg_integer(), [remote_output()]) ->
                         {ok, [remote_output()]} | {error, term()}.
save_hdfs(_JobName, [], _SaveInfo, _, _, Acc) ->
    {ok, Acc};

save_hdfs(JobName, [{L, Loc, Sz} = _H | Rest], SaveInfo, TaskId, Index, Acc) ->
    ["hdfs", NameNode, User, HdfsDir] = string:tokens(SaveInfo, [$,]),
    LocalPath = disco:joburl_to_localpath(Loc),
    Name = HdfsDir ++ hdfs:get_compliant_name(JobName) ++
        "/" ++ integer_to_list(TaskId) ++
        "_" ++ integer_to_list(Index),

    case hdfs:save_to_hdfs(NameNode, Name, User, LocalPath) of
        {ok, HdfsLoc} ->
            save_hdfs(JobName, Rest, SaveInfo, TaskId, Index + 1, [{L, Sz, [HdfsLoc]}|Acc]);
        error ->
            {error, save_failed}
    end.

-spec parse_index(binary()) -> [{label(), url(), data_size()}].
parse_index(Index) ->
    case re:run(Index, "(.*?) (.*?) (.*?)\n",
                [global, {capture, all_but_first, binary}])
    of  {match, Lines} ->
            [{list_to_integer(binary_to_list(L)),
              Url,
              list_to_integer(binary_to_list(Sz))}
             || [L, Url, Sz] <- Lines];
        nomatch ->
            []
    end.

-spec save_locals([{label(), url(), data_size()}], integer(), node(),
                  jobname(), task_id(), task_run_id(), [remote_output()]) ->
                         {ok, [remote_output()]} | {error, term()}.
save_locals([], _K, _M, _JN, _Tid, _Rid, Saved) ->
    {ok, Saved};
save_locals([{L, Loc, Sz} = _H | Rest], K, M, JN, TaskId, RunId, Acc) ->
    LocalPath = disco:joburl_to_localpath(Loc),
    % Since this task can be re-run, name blob replicas such that
    % collisions across re-runs of the same task are avoided.  Unused
    % blobs will not be in the final tag, and will be garbage
    % collected.
    BlobBase = disco:format("~s:~p:~p:~s",
                            [JN, TaskId, RunId, filename:basename(LocalPath)]),
    BlobName = list_to_binary(ddfs_util:make_valid_name(BlobBase)),
    Blob = ddfs_util:pack_objname(BlobName, now()),
    % TODO: Optimize new_blob to handle hints so that local blob
    % copies can be created.
    case ddfs_save(M, Blob, LocalPath, K, ?MAX_SAVE_ATTEMPTS, {[], []}) of
        {ok, Saved} ->
            Res = {L, Sz, [{U, disco:preferred_host(U)} || U <- Saved]},
            save_locals(Rest, K, M, JN, TaskId, RunId, [Res|Acc]);
        {error, _} = Err ->
            Err
    end.

-spec ddfs_save(node(), object_name(), path(), integer(), integer(),
                {[url()], [node()]}) -> {ok, [url()]} | {error, term()}.
ddfs_save(_M, _B, _P, _K, 0, _) ->
    {error, too_many_save_failures};
ddfs_save(M, BlobName, Path, K, Attempts, {Acc, Exclude}) ->
    case ddfs_master:new_blob({ddfs_master, M}, BlobName, K, [], Exclude) of
        {ok, Dests} ->
            error_logger:info_msg("Replicating ~s from ~p to ~p (excluding ~p)",
                                  [BlobName, Path, Dests, Exclude]),
            Res =
                [save_result(Path, D, ddfs_http:http_put(Path, D, ?PUT_TIMEOUT))
                 || D <- Dests],
            Acc1 = [U || {ok, U} <- Res] ++ Acc,
            case length(Acc1) < K of
                true ->
                    X = [N || D <- Dests,
                              N = disco:slave_for_url(list_to_binary(D))
                                  =/= false],
                    ddfs_save(M, BlobName, Path, K, Attempts - 1,
                              {Acc1, X ++ Exclude});
                false ->
                    {ok, Acc1}
            end;
        E ->
            {error, E}
    end.

-spec save_result(path(), nonempty_string(), {ok, binary()} | {error, _}) ->
                         {error, term()} | {ok, url()}.
save_result(Loc, D, {error, _} = E) ->
    error_logger:error_msg("Error saving ~p to ~p: ~p", [Loc, D, E]),
    E;
save_result(Loc, D, {ok, Body}) ->
    Resp = try mochijson2:decode(Body)
           catch _ -> invalid; _:_ -> invalid
           end,
    case Resp of
        invalid ->
            error_logger:error_msg("Invalid response on saving ~p to ~p: ~p",
                                   [Loc, D, Resp]),
            {error, {server_error, Body}};
        Resp when is_binary(Resp) ->
            case ddfs_util:parse_url(Resp) of
                not_ddfs ->
                    error_logger:error_msg("Invalid final url for ~p to ~p: ~p",
                                           [Loc, D, Resp]),
                    {error, {server_error, Body}};
                _ ->
                    {ok, Resp}
            end;
        _ ->
            error_logger:error_msg("Invalid response for ~p to ~p: ~p",
                                   [Loc, D, Resp]),
            {error, {server_body, Resp}}
    end.
