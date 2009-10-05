
-module(handle_ctrl).
-export([handle/2]).

-define(HTTP_HEADER, "HTTP/1.1 200 OK\n"
                     "Status: 200 OK\n"
                     "Content-type: text/plain\n\n").

job_status(J) ->
        case gen_server:call(event_server, {get_results, J}) of
                {active, _} -> <<"job_active">>;
                {dead, _} -> <<"job_died">>;
                {ready, _, _} -> <<"job_ready">>
        end.

op("save_config_table", _Query, Json) ->
        disco_config:save_config_table(Json);

op("load_config_table", _Query, _Json) -> 
        disco_config:get_config_table();

op("joblist", _Query, _Json) ->
        {ok, Lst} = gen_server:call(event_server, get_jobnames),
        {ok, Prios} = gen_server:call(sched_policy, current_priorities),

        TLst = lists:map(fun({J, _, _}) ->
                {case lists:keysearch(J, 1, Prios) of
                        false -> 1.0;
                        {value, {_, Prio}} -> Prio
                 end, job_status(J), list_to_binary(J)}
        end, Lst),
        {ok, lists:keysort(1, TLst)};

op("jobinfo", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {ok, {Nodes, Tasks}} =
                gen_server:call(disco_server, {get_active, Name}),
        {ok, {TStamp, Pid, JobNfo, Res, Ready, Failed}} =
                gen_server:call(event_server, {get_jobinfo, Name}),
        {ok, render_jobinfo(TStamp, Pid, JobNfo, Nodes, 
                Res, Tasks, Ready, Failed)};

op("parameters", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {ok, MasterUrl} = application:get_env(disco_url),
        {relo, [MasterUrl, "/", disco_server:jobhome(Name), "params"]};

op("rawevents", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {ok, MasterUrl} = application:get_env(disco_url),
        {relo, [MasterUrl, "/", disco_server:jobhome(Name), "events"]};

op("oob_get", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {value, {_, Key}} = lists:keysearch("key", 1, Query),
        Proxy = lists:keysearch("proxy", 1, Query),
        case {Proxy, gen_server:call(oob_server, {fetch, 
                        list_to_binary(Name), list_to_binary(Key)})} of
                {P, {ok, {Node, Path}}} when P == false;
                                P == {value, {"proxy", "0"}} -> 
                        {relo, ["http://", Node, ":", 
                                os:getenv("DISCO_PORT"), "/", Path]};
                {_, {ok, {Node, Path}}} ->
                        {ok, MasterUrl} = application:get_env(disco_url),
                        [_, H|_] = string:tokens(MasterUrl, "/"),
                        {relo, ["http://", H,
                                "/disco/node/", Node, "/", Path]};
                {_, error} -> not_found
        end;

op("oob_list", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        case gen_server:call(oob_server, {list, list_to_binary(Name)}) of
                {ok, Keys} -> {ok, Keys};
                error -> not_found
        end;
        
op("jobevents", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {value, {_, NumS}} = lists:keysearch("num", 1, Query),
        Num = list_to_integer(NumS),
        Q = case lists:keysearch("filter", 1, Query) of
                false -> "";
                {value, {_, F}} -> string:to_lower(F)
        end,
        {ok, Ev} = gen_server:call(event_server,
                {get_job_events, Name, string:to_lower(Q), Num}),
        {raw, Ev};

op("nodeinfo", _Query, _Json) ->
        {ok, {Available, Active}} = 
                gen_server:call(disco_server, {get_nodeinfo, all}),
        ActiveB = lists:map(fun({Node, JobName}) ->
                {obj, [{node, list_to_binary(Node)},
                       {jobname, list_to_binary(JobName)}]}
        end, Active),
        {ok, {obj, [{available, Available}, {active, ActiveB}]}};

op("kill_job", _Query, Json) ->
        JobName = binary_to_list(Json),
        gen_server:call(disco_server, {kill_job, JobName}),
        {ok, <<>>};

op("purge_job", _Query, Json) ->
        JobName = binary_to_list(Json),
        gen_server:cast(disco_server, {purge_job, JobName}),
        {ok, <<>>};

op("clean_job", _Query, Json) ->
        JobName = binary_to_list(Json),
        gen_server:call(disco_server, {clean_job, JobName}),
        {ok, <<>>};

op("get_results", _Query, Json) ->
        [Timeout, Names] = Json,
        S = [{N, gen_server:call(event_server,
                {get_results, binary_to_list(N)})} || N <- Names],
        {ok, [[N, status_msg(M)] || {N, M} <- wait_jobs(S, Timeout)]};

op("get_blacklist", _Query, _Json) ->
        {ok, {A, _}} = gen_server:call(disco_server, {get_nodeinfo, all}),
        {ok, [N0 || {N0, true} <- lists:map(fun({obj, L}) ->
                {value, {_, N}} = lists:keysearch(node, 1, L),
                {value, {_, B}} = lists:keysearch(blacklisted, 1, L),
                {N, B}
        end, A)]};

op("blacklist", _Query, Json) ->
        Node = binary_to_list(Json),
        gen_server:call(disco_server, {blacklist, Node}),
        {ok, <<>>};

op("whitelist", _Query, Json) ->
        Node = binary_to_list(Json),
        gen_server:call(disco_server, {whitelist, Node}),
        {ok, <<>>};

op("get_settings", _Query, _Json) ->
        L = [max_failure_rate],
        {ok, {obj, lists:filter(fun(X) -> is_tuple(X) end, 
                lists:map(fun(S) ->
                        case application:get_env(disco, S) of
                                {ok, V} -> {S, V};
                                _ -> false
                        end
                end, L))}};

op("save_settings", _Query, Json) ->
        {obj, Lst} = Json,
        {ok, App} = application:get_application(),
        lists:foreach(fun({Key, Val}) ->
                update_setting(Key, Val, App)
        end, Lst),
        {ok, <<"Settings saved">>}.

update_setting("max_failure_rate", Val, App) ->
        ok = application:set_env(App, max_failure_rate,
                list_to_integer(binary_to_list(Val)));
        
update_setting(Key, Val, _) ->
        error_logger:info_report([{"Unknown setting", Key, Val}]).

handle(Socket, Msg) ->
        {value, {_, Script}} = lists:keysearch(<<"SCRIPT_NAME">>, 1, Msg),
        {value, {_, Query}} = lists:keysearch(<<"QUERY_STRING">>, 1, Msg),
        {value, {_, CLenStr}} = lists:keysearch(<<"CONTENT_LENGTH">>, 1, Msg),
        CLen = list_to_integer(binary_to_list(CLenStr)),
        if CLen > 0 ->
                {ok, PostData} = gen_tcp:recv(Socket, CLen, 30000),
                {ok, Json, _Rest} = json:decode(PostData);
        true ->
                Json = none
        end,
        
        handle_job:set_disco_url(application:get_env(disco_url), Msg),
        
        Op = lists:last(string:tokens(binary_to_list(Script), "/")),
        Reply = case catch op(Op,
                        httpd:parse_query(binary_to_list(Query)), Json) of
                {ok, Res} -> [?HTTP_HEADER, json:encode(Res)];
                {raw, Res} -> [?HTTP_HEADER, Res];
                {relo, Loc} -> ["HTTP/1.1 302 ok\nLocation: ", Loc, "\n\n"];
                not_found -> "HTTP/1.1 404 not found\n"
                             "Status: 404 Not found\n\nNot found.";
                R -> error_logger:info_report({"Request failed", Op, R}),
                    "HTTP/1.1 500 internal server error\n"
                    "Status: 500 Internal server error\n\n"
                    "Internal server error."
        end,
        gen_tcp:send(Socket, Reply).


count_maps(L) ->
        {M, N} = lists:foldl(fun
                ("map", {M, N}) -> {M + 1, N + 1};
                (["map"], {M, N}) -> {M + 1, N + 1};
                (_, {M, N}) -> {M, N + 1}
        end, {0, 0}, L),
        {M, N - M}.

render_jobinfo(Tstamp, JobPid, [{_NMap, NRed, DoRed, Inputs}],
        Nodes, Res, Tasks, Ready, Failed) ->

        {NMapRun, NRedRun} = count_maps(Tasks),
        {NMapDone, NRedDone} = count_maps(Ready),
        {NMapFail, NRedFail} = count_maps(Failed),
        
        R = case {Res, is_process_alive(JobPid)} of
                {_, true} -> <<"active">>;
                {[], false} -> <<"dead">>;
                {_, false} -> <<"ready">>
        end,
        {obj, [{timestamp, Tstamp}, 
               {active, R},
               {mapi, [length(Inputs) - (NMapDone + NMapRun),
                        NMapRun, NMapDone, NMapFail]},
               {redi, [NRed - (NRedDone + NRedRun),
                        NRedRun, NRedDone, NRedFail]},
               {reduce, DoRed},
               {results, lists:flatten(Res)},
               {inputs, lists:sublist(Inputs, 100)},
               {nodes, lists:map(fun erlang:list_to_binary/1, Nodes)}
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


