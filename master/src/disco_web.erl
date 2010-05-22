
-module(disco_web).
-export([op/3]).

-include("task.hrl").
-include("config.hrl").

op('POST', "/disco/job/" ++ _, Req) ->
    BodySize = list_to_integer(Req:get_header_value("content-length")),
    if BodySize > ?MAX_JOB_PACKET ->
        Req:respond({413, [], ["Job packet too large"]});
    true ->
        Body = Req:recv_body(?MAX_JOB_PACKET),
        case catch job_coordinator:new(Body) of
            {ok, JobName} ->
                reply({ok, [<<"ok">>, list_to_binary(JobName)]}, Req);
            {'EXIT', Error} ->
                error_logger:warning_report({"could not start job", Error}),
                reply({ok, [<<"error">>, <<"could not start job">>]}, Req)
        end
    end;

op('POST', "/disco/ctrl/" ++ Op, Req) ->
    Json = mochijson2:decode(Req:recv_body(?MAX_JSON_POST)),
    reply(postop(Op, Json), Req);

op('GET', "/disco/ctrl/" ++ Op, Req) ->
    Query = Req:parse_qs(),
    Name =
        case lists:keysearch("name", 1, Query) of
            {value, {_, N}} -> N;
            _ -> false
        end,
    reply(getop(Op, {Query, Name}), Req);

op('GET', Path, Req) ->
    ddfs_get:serve_disco_file(Path, Req);

op(_, _, Req) ->
    Req:not_found().

reply({ok, Data}, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)});
reply({raw, Data}, Req) ->
    Req:ok({"text/plain", [], Data});
reply({relo, Loc}, Req) ->
    Req:respond({302, [{"Location", Loc}], <<>>});
reply({file, File, Docroot}, Req) ->
    Req:serve_file(File, Docroot);
reply(not_found, Req) ->
    Req:not_found();
reply(_, Req) ->
    Req:respond({500, [], ["Internal server error"]}).

getop("load_config_table", _Query) ->
    disco_config:get_config_table();

getop("joblist", _Query) ->
    {ok, Jobs} = gen_server:call(event_server, get_jobs),
    {ok, [[1000000 * MSec + Sec, list_to_binary(atom_to_list(Status)), Name] ||
        {Name, Status, {MSec, Sec, _USec}, _Pid}
            <- lists:reverse(lists:keysort(3, Jobs))]};

getop("jobinfo", {_Query, Name}) ->
    {ok, {Nodes, Tasks}} =
        gen_server:call(disco_server, {get_active, Name}),
    {ok, {TStamp, Pid, JobNfo, Res, Ready, Failed}} =
        gen_server:call(event_server, {get_jobinfo, Name}),
    {ok, render_jobinfo(TStamp, Pid, JobNfo, Nodes,
        Res, Tasks, Ready, Failed)};

getop("parameters", {_Query, Name}) ->
    job_file(Name, "params");

getop("rawevents", {_Query, Name}) ->
    job_file(Name, "events");

getop("jobevents", {Query, Name}) ->
    {value, {_, NumS}} = lists:keysearch("num", 1, Query),
    Num = list_to_integer(NumS),
    Q = case lists:keysearch("filter", 1, Query) of
        false -> "";
        {value, {_, F}} -> string:to_lower(F)
    end,
    {ok, Ev} = gen_server:call(event_server,
        {get_job_events, Name, string:to_lower(Q), Num}),
    {raw, Ev};

getop("nodeinfo", _Query) ->
    {ok, {Available, Active}} =
        gen_server:call(disco_server, {get_nodeinfo, all}),
    ActiveB = lists:map(fun({Node, JobName}) ->
        {struct, [{node, list_to_binary(Node)},
               {jobname, list_to_binary(JobName)}]}
    end, Active),
    {ok, {struct, [{available, Available}, {active, ActiveB}]}};

getop("get_blacklist", _Query) ->
    {ok, {A, _}} = gen_server:call(disco_server, {get_nodeinfo, all}),
    {ok, [N0 || {N0, true} <- lists:map(fun({struct, L}) ->
        {value, {_, N}} = lists:keysearch(node, 1, L),
        {value, {_, B}} = lists:keysearch(blacklisted, 1, L),
        {N, B}
    end, A)]};

getop("get_settings", _Query) ->
    L = [max_failure_rate],
    {ok, {struct, lists:filter(fun(X) -> is_tuple(X) end,
        lists:map(fun(S) ->
            case application:get_env(disco, S) of
                {ok, V} -> {S, V};
                _ -> false
            end
        end, L))}};

getop("get_mapresults", {_Query, Name}) ->
    case gen_server:call(event_server, {get_map_results, Name}) of
        {ok, Res} ->
            {ok, Res};
        _ ->
            not_found
    end;

getop(_, _) -> not_found.

postop("kill_job", Json) ->
    JobName = binary_to_list(Json),
    gen_server:call(disco_server, {kill_job, JobName}),
    {ok, <<>>};

postop("purge_job", Json) ->
    JobName = binary_to_list(Json),
    gen_server:cast(disco_server, {purge_job, JobName}),
    {ok, <<>>};

postop("clean_job", Json) ->
    JobName = binary_to_list(Json),
    gen_server:call(disco_server, {clean_job, JobName}),
    {ok, <<>>};

postop("get_results", Json) ->
    [Timeout, Names] = Json,
    S = [{N, gen_server:call(event_server,
        {get_results, binary_to_list(N)})} || N <- Names],
    {ok, [[N, status_msg(M)] || {N, M} <- wait_jobs(S, Timeout)]};

postop("blacklist", Json) ->
    Node = binary_to_list(Json),
    gen_server:call(disco_server, {blacklist, Node, manual}),
    {ok, <<>>};

postop("whitelist", Json) ->
    Node = binary_to_list(Json),
    gen_server:call(disco_server, {whitelist, Node, any}),
    {ok, <<>>};

postop("save_config_table", Json) ->
    disco_config:save_config_table(Json);

postop("save_settings", Json) ->
    {struct, Lst} = Json,
    {ok, App} = application:get_application(),
    lists:foreach(fun({Key, Val}) ->
        update_setting(Key, Val, App)
    end, Lst),
    {ok, <<"Settings saved">>};

postop(_, _) -> not_found.

job_file(Name, File) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    Home = disco_server:jobhome(Name),
    {file, File, filename:join([Root, Home])}.

update_setting("max_failure_rate", Val, App) ->
    ok = application:set_env(App, max_failure_rate,
        list_to_integer(binary_to_list(Val)));

update_setting(Key, Val, _) ->
    error_logger:info_report([{"Unknown setting", Key, Val}]).

count_maps(L) ->
    {M, N} = lists:foldl(fun
        ("map", {M, N}) -> {M + 1, N + 1};
        (["map"], {M, N}) -> {M + 1, N + 1};
        (_, {M, N}) -> {M, N + 1}
    end, {0, 0}, L),
    {M, N - M}.

render_jobinfo(Tstamp, JobPid, JobInfo, Nodes, Res, Tasks, Ready, Failed) ->
    {NMapRun, NRedRun} = count_maps(Tasks),
    {NMapDone, NRedDone} = count_maps(Ready),
    {NMapFail, NRedFail} = count_maps(Failed),

    R = case {Res, is_process_alive(JobPid)} of
        {_, true} -> <<"active">>;
        {[], false} -> <<"dead">>;
        {_, false} -> <<"ready">>
    end,

    MapI = if JobInfo#jobinfo.map ->
            length(JobInfo#jobinfo.inputs) - (NMapDone + NMapRun);
        true -> 0 end,
    RedI = if JobInfo#jobinfo.reduce ->
            JobInfo#jobinfo.nr_reduce - (NRedDone + NRedRun);
           true -> 0 end,

    {struct, [{timestamp, Tstamp},
           {active, R},
           {mapi, [MapI, NMapRun, NMapDone, NMapFail]},
           {redi, [RedI, NRedRun, NRedDone, NRedFail]},
           {reduce, JobInfo#jobinfo.reduce},
           {results, lists:flatten(Res)},
           {inputs, lists:sublist(JobInfo#jobinfo.inputs, 100)},
           {nodes, [list_to_binary(N) || N <- Nodes]}
    ]}.

status_msg(invalid_job) -> [<<"unknown job">>, []];
status_msg({ready, _, Res}) -> [<<"ready">>, Res];
status_msg({active, _}) -> [<<"active">>, []];
status_msg({dead, _}) -> [<<"dead">>, []].

wait_jobs(Jobs, Timeout) ->
    case [erlang:monitor(process, Pid) || {_, {active, Pid}} <- Jobs] of
        [] -> Jobs;
        _ ->
            receive
                {'DOWN', _, _, _, _} -> ok
            after Timeout -> ok
            end,
            [{N, gen_server:call(event_server,
                {get_results, binary_to_list(N)})} ||
                    {N, _} <- Jobs]
    end.
