-module(disco_web).
-export([op/3]).

-include("disco.hrl").
-include("config.hrl").

-spec op(atom(), string(), module()) -> _.
op('GET', "/disco/version", Req) ->
    {ok, Vsn} = application:get_key(vsn),
    reply({ok, list_to_binary(Vsn)}, Req);

op('POST', "/disco/job/" ++ _, Req) ->
    BodySize = list_to_integer(Req:get_header_value("content-length")),
    if BodySize > ?MAX_JOB_PACKET ->
            Req:respond({413, [], ["Job packet too large"]});
    true ->
            Body = Req:recv_body(?MAX_JOB_PACKET),
            Reply =
                try
                    {ok, JobName} = job_coordinator:new(Body),
                    [<<"ok">>, list_to_binary(JobName)]
                catch K:E ->
                        ErrorString = disco:format("Job failed to start: ~p:~p", [K, E]),
                        lager:warning("Job failed to start: ~p:~p", [K, E]),
                        [<<"error">>, list_to_binary(ErrorString)]
                end,
            reply({ok, Reply}, Req)
    end;

op('POST', "/disco/ctrl/" ++ Op, Req) ->
    Json = mochijson2:decode(Req:recv_body(?MAX_JSON_POST)),
    reply(postop(Op, Json), Req);

op('GET', "/disco/ctrl/" ++ Op, Req) ->
    Query = Req:parse_qs(),
    Name =
        case lists:keyfind("name", 1, Query) of
            {_, N} -> N;
            _ -> false
        end,
    reply(getop(Op, {Query, Name}), Req);

op('GET', Path, Req) ->
    DiscoRoot = disco:get_setting("DISCO_DATA"),
    ddfs_get:serve_disco_file(DiscoRoot, Path, Req);

op(_, _, Req) ->
    Req:not_found().

reply({ok, Data}, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)});
reply({raw, Data}, Req) ->
    Req:ok({"text/plain", [], Data});
reply({file, File, Docroot}, Req) ->
    Req:serve_file(File, Docroot);
reply(not_found, Req) ->
    Req:not_found();
reply({error, E}, Req) ->
    Req:respond({400, [], mochijson2:encode(E)}).

getop("load_config_table", _Query) ->
    disco_config:get_config_table();

getop("joblist", _Query) ->
    {ok, Jobs} = gen_server:call(event_server, get_jobs),
    {ok, [[1000000 * MSec + Sec, list_to_binary(atom_to_list(Status)), Name]
          || {Name, Status, {MSec, Sec, _USec}, _Pid}
                 <- lists:reverse(lists:keysort(3, Jobs))]};

getop("jobinfo", {_Query, JobName}) ->
    {ok, Active} = disco_server:get_active(JobName),
    case gen_server:call(event_server, {get_jobinfo, JobName}) of
        {ok, JobInfo} ->
            HostInfo = lists:unzip([{Host, M}
                                    || {Host, #task{mode = M}} <- Active]),
            {ok, render_jobinfo(JobInfo, HostInfo)};
        invalid_job ->
            not_found
    end;

getop("parameters", {_Query, Name}) ->
    job_file(Name, "jobfile");

getop("rawevents", {_Query, Name}) ->
    job_file(Name, "events");

getop("jobevents", {Query, Name}) ->
    {_, NumS} = lists:keyfind("num", 1, Query),
    Num = list_to_integer(NumS),
    Q = case lists:keyfind("filter", 1, Query) of
            false -> "";
            {_, F} -> string:to_lower(F)
        end,
    {ok, Ev} = gen_server:call(event_server,
                               {get_job_events, Name, string:to_lower(Q), Num}),
    {raw, Ev};

getop("nodeinfo", _Query) ->
    {ok, Active} = disco_server:get_active(all),
    {ok, DiscoNodes} = disco_server:get_nodeinfo(all),
    {ok, DDFSNodes} = ddfs_master:get_nodeinfo(all),
    ActiveNodeInfo = lists:foldl(fun ({Host, #task{jobname = JobName}}, Dict) ->
                                         dict:append(Host,
                                                     list_to_binary(JobName),
                                                     Dict)
                                 end, dict:new(), Active),
    DiscoNodeInfo = dict:from_list([{N#nodeinfo.name,
                                     [{job_ok, N#nodeinfo.stats_ok},
                                      {data_error, N#nodeinfo.stats_failed},
                                      {error, N#nodeinfo.stats_crashed},
                                      {max_workers, N#nodeinfo.slots},
                                      {connected, N#nodeinfo.connected},
                                      {blacklisted, N#nodeinfo.blacklisted}]}
                                    || N <- DiscoNodes]),
    NodeInfo = lists:foldl(fun ({Node, {Free, Used}}, Dict) ->
                                   dict:append_list(disco:host(Node),
                                                    [{diskfree, Free},
                                                     {diskused, Used}],
                                                    Dict)
                           end,
                           dict:merge(fun (_Key, Tasks, Other) ->
                                              [{tasks, Tasks}|Other]
                                      end,
                                      ActiveNodeInfo, DiscoNodeInfo),
                           DDFSNodes),
    {ok, {struct, [{K, {struct, Vs}} || {K, Vs} <- dict:to_list(NodeInfo)]}};

getop("get_blacklist", _Query) ->
    {ok, Nodes} = disco_server:get_nodeinfo(all),
    {ok, [list_to_binary(N#nodeinfo.name)
          || N <- Nodes, N#nodeinfo.blacklisted]};

getop("get_gc_blacklist", _Query) ->
    {ok, Nodes} = ddfs_master:gc_blacklist(),
    {ok, [list_to_binary(disco:host(N)) || N <- Nodes]};

getop("get_settings", _Query) ->
    L = [max_failure_rate],
    {ok, {struct, lists:filter(fun(X) -> is_tuple(X) end,
                               lists:map(
                                 fun(S) ->
                                     case application:get_env(disco, S) of
                                         {ok, V} -> {S, V};
                                         _ -> false
                                     end
                                 end, L))}};

getop("get_mapresults", {_Query, Name}) ->
    case gen_server:call(event_server, {get_map_results, Name}) of
        {ok, _Res} = OK ->
            OK;
        _ ->
            not_found
    end;

getop(_, _) -> not_found.

-spec validate_payload(nonempty_string(), json_validator:spec(),
                       term(), fun((term()) -> T)) -> T.
validate_payload(_Op, Spec, Payload, Fun) ->
    case json_validator:validate(Spec, Payload) of
        ok -> Fun(Payload);
        {error, E} ->
            Msg = list_to_binary(json_validator:error_msg(E)),
            {error, Msg}
    end.

postop("kill_job", Json) ->
    validate_payload("kill_job", string, Json,
                     fun(J) ->
                             JobName = binary_to_list(J),
                             disco_server:kill_job(JobName),
                             {ok, <<>>}
                     end);

postop("purge_job", Json) ->
    validate_payload("purge_job", string, Json,
                     fun(J) ->
                             JobName = binary_to_list(J),
                             disco_server:purge_job(JobName),
                             {ok, <<>>}
                     end);

postop("clean_job", Json) ->
    validate_payload("clean_job", string, Json,
                     fun(J) ->
                             JobName = binary_to_list(J),
                             disco_server:clean_job(JobName),
                             {ok, <<>>}
                     end);

postop("get_results", Json) ->
    Results = fun(N) -> gen_server:call(event_server, {get_results, N}) end,
    validate_payload("get_results", {array, [integer, {hom_array, string}]}, Json,
                     fun(J) ->
                             [Timeout, Names] = J,
                             S = [{N, Results(binary_to_list(N))} || N <- Names],
                             {ok, [[N, status_msg(M)]
                                   || {N, M} <- wait_jobs(S, Timeout)]}
                     end);

postop("blacklist", Json) ->
    validate_payload("blacklist", string, Json,
                     fun(J) ->
                             Node = binary_to_list(J),
                             disco_config:blacklist(Node),
                             {ok, <<>>}
                     end);

postop("whitelist", Json) ->
    validate_payload("whitelist", string, Json,
                     fun(J) ->
                             Node = binary_to_list(J),
                             disco_config:whitelist(Node),
                             {ok, <<>>}
                     end);

postop("gc_blacklist", Json) ->
    validate_payload("gc_blacklist", string, Json,
                     fun(J) ->
                             Node = binary_to_list(J),
                             disco_config:gc_blacklist(Node),
                             {ok, <<>>}
                     end);

postop("gc_whitelist", Json) ->
    validate_payload("gc_whitelist", string, Json,
                     fun(J) ->
                             Node = binary_to_list(J),
                             disco_config:gc_whitelist(Node),
                             {ok, <<>>}
                     end);

postop("save_config_table", Json) ->
    validate_payload("save_config_table", {hom_array, {array, [string, string]}},
                     Json, fun(J) -> disco_config:save_config_table(J) end);

postop("save_settings", Json) ->
    validate_payload("save_settings", {object, []}, Json,
                     fun(J) ->
                             {struct, Lst} = J,
                             {ok, App} = application:get_application(),
                             lists:foreach(fun({Key, Val}) ->
                                                   update_setting(Key, Val, App)
                                           end, Lst),
                             {ok, <<"Settings saved">>}
                     end);

postop(_, _) -> not_found.

job_file(Name, File) ->
    Root = disco:get_setting("DISCO_MASTER_ROOT"),
    Home = disco:jobhome(Name),
    {file, File, filename:join([Root, Home])}.

update_setting(<<"max_failure_rate">>, Val, App) ->
    ok = application:set_env(App, max_failure_rate,
                             list_to_integer(binary_to_list(Val)));

update_setting(Key, Val, _) ->
    lager:info("Unknown setting: ~p = ~p", [Key, Val]).

count_maps(L) ->
    {M, N} = lists:foldl(fun ("map", {M, N}) ->
                                 {M + 1, N + 1};
                             (["map"], {M, N}) ->
                                 {M + 1, N + 1};
                             (_, {M, N}) ->
                                 {M, N + 1}
                         end, {0, 0}, L),
    {M, N - M}.

render_jobinfo({Timestamp, Pid, JobInfo, Results, Ready, Failed},
               {Hosts, Modes}) ->
    {NMapRun, NRedRun} = count_maps(Modes),
    {NMapDone, NRedDone} = count_maps(Ready),
    {NMapFail, NRedFail} = count_maps(Failed),

    Status = case is_process_alive(Pid) of
                 true ->
                     <<"active">>;
                 false when Results == [] ->
                     <<"dead">>;
                 false ->
                     <<"ready">>
             end,

    MapI = if
               JobInfo#jobinfo.map ->
                   length(JobInfo#jobinfo.inputs) - (NMapDone + NMapRun);
               true ->
                   0
           end,
    RedI = if
               JobInfo#jobinfo.reduce ->
                   JobInfo#jobinfo.nr_reduce - (NRedDone + NRedRun);
               true -> 0
           end,

    {struct, [{timestamp, Timestamp},
              {active, Status},
              {mapi, [MapI, NMapRun, NMapDone, NMapFail]},
              {redi, [RedI, NRedRun, NRedDone, NRedFail]},
              {reduce, JobInfo#jobinfo.reduce},
              {results, lists:flatten(Results)},
              {inputs, lists:sublist(JobInfo#jobinfo.inputs, 100)},
              {worker, JobInfo#jobinfo.worker},
              {hosts, [list_to_binary(Host) || Host <- Hosts]},
              {owner, JobInfo#jobinfo.owner}
             ]}.

status_msg(invalid_job) -> [<<"unknown job">>, []];
status_msg({ready, _, Results}) -> [<<"ready">>, Results];
status_msg({active, _}) -> [<<"active">>, []];
status_msg({dead, _}) -> [<<"dead">>, []].

wait_jobs(Jobs, Timeout) ->
    case [erlang:monitor(process, Pid) || {_, {active, Pid}} <- Jobs] of
        [] -> Jobs;
        _ ->
            receive {'DOWN', _, _, _, _} -> ok
            after Timeout -> ok
            end,
            [{N, gen_server:call(event_server,
                                 {get_results, binary_to_list(N)})}
             || {N, _} <- Jobs]
    end.
