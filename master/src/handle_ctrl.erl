
-module(handle_ctrl).
-export([handle/2]).

-define(HTTP_HEADER, "HTTP/1.1 200 OK\n"
                     "Status: 200 OK\n"
                     "Content-type: text/plain\n\n").

op("save_config_table", _Query, Json) ->
        disco_config:save_config_table(Json);

op("load_config_table", _Query, _Json) -> 
        disco_config:get_config_table();

op("joblist", _Query, _Json) ->
        {ok, Lst} = gen_server:call(disco_server, get_jobnames),
        Nu = now(),
        TLst = lists:map(fun([J, T, P]) ->
                {round(timer:now_diff(Nu, T) / 1000000),
                        process_status(J, is_process_alive(P)),
                        list_to_binary(J)}
        end, Lst),
        {ok, lists:keysort(1, TLst)};

op("jobinfo", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {ok, {MapNfo, Res, Nodes, Tasks, Ready, Failed}}
                = gen_server:call(disco_server, {get_jobinfo, Name}),
        {ok, render_jobinfo(MapNfo, Nodes, Res, Tasks, Ready, Failed)};

op("jobevents", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        case lists:keysearch("find", 1, Query) of
                false -> range_events(Name, Query);
                {value, {_, Find}} -> search_events(Name, Find)
        end;

op("nodeinfo", _Query, _Json) ->
        {ok, {Available, Active}} = 
                gen_server:call(disco_server, {get_nodeinfo, all}),
        ActiveB = lists:map(fun([Node, JobName]) ->
                {obj, [{node, list_to_binary(Node)},
                       {jobname, list_to_binary(JobName)}]}
        end, Active),
        {ok, {obj, [{available, Available}, {active, ActiveB}]}};

op("kill_job", _Query, Json) ->
        JobName = binary_to_list(Json),
        gen_server:call(disco_server, {kill_job, JobName}),
        {ok, <<>>};

op("clean_job", _Query, Json) ->
        JobName = binary_to_list(Json),
        gen_server:call(disco_server, {clean_job, JobName}),
        {ok, <<>>};

op("get_results", Query, _Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        case gen_server:call(disco_server, {get_results, Name}) of
                {ok, [], []} -> {ok, [<<"unknown job">>, []]};
                {ok, [Pid], []} -> V = is_process_alive(Pid),
                                 if V -> {ok, [<<"active">>, []]};
                                 true -> {ok, [<<"dead">>, []]} end;
                {ok, _, Events} -> {ok, [<<"ready">>, Events]}
        end;

op("get_blacklist", _Query, _Json) ->
        {ok, lists:map(fun({Node, _}) -> list_to_binary(Node)
                end, ets:tab2list(blacklist))};

op("blacklist", _Query, Json) ->
        Node = binary_to_list(Json),
        case ets:lookup(config_table, Node) of
                [] -> {ok, <<"Unknown node">>};
                _ -> gen_server:call(disco_server, {blacklist, Node}),
                     {ok, <<"Node blacklisted">>}
        end;

op("whitelist", _Query, Json) ->
        Node = binary_to_list(Json),
        case ets:lookup(blacklist, Node) of
                [] -> {ok, <<"Node not on the blacklist">>};
                _ -> gen_server:call(disco_server, {whitelist, Node}),
                     {ok, <<"Node whitelisted">>}
        end;

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
        {value, {_, Script}} = lists:keysearch("SCRIPT_NAME", 1, Msg),
        {value, {_, Query}} = lists:keysearch("QUERY_STRING", 1, Msg),
        {value, {_, CLenStr}} = lists:keysearch("CONTENT_LENGTH", 1, Msg),
        CLen = list_to_integer(CLenStr),
        if CLen > 0 ->
                {ok, PostData} = gen_tcp:recv(Socket, CLen, 0),
                {ok, Json, _Rest} = json:decode(PostData);
        true ->
                Json = none
        end,
        Op = lists:last(string:tokens(Script, "/")),
        {ok, Res} = op(Op, httpd:parse_query(Query), Json),
        gen_tcp:send(Socket, [?HTTP_HEADER, json:encode(Res)]).

range_events(Name, Query) ->
        {value, {_, OffsS}} = lists:keysearch("offs", 1, Query),
        {value, {_, NumS}} = lists:keysearch("num", 1, Query),
        Offs = list_to_integer(OffsS),
        Num = list_to_integer(NumS),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> {ok, [Offs, Num, false, []]};
                {ok, Events} -> {ok, [Offs, Num, Offs + Num < length(Events),
                        page(lists:reverse(Events), Offs, Num)]}
        end.

search_events(Name, Query) ->
        Q = string:to_lower(Query),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> {ok, [-1, 0, false, []]};
                {ok, Events} -> {ok, [-1, 0, false, render_page(
                        lists:filter(fun({_, _, [M|_]}) ->
                                string:str(string:to_lower(
                                        binary_to_list(M)), Q) > 0
                        end, lists:reverse(Events)))]}
        end.

process_status(_Jobname, true) -> <<"job_active">>;
process_status(JobName, false) ->
        case gen_server:call(disco_server, {get_results, JobName}) of
                {ok, _, []} -> <<"job_died">>;
                {ok, _, _} -> <<"job_ready">>
        end.

page(E, O, N) when O < 0; N < 0 ->
        render_page(E);
page(E, O, N) when O =< length(E) ->
        render_page(lists:sublist(E, O + 1, N));
page(E, O, _N) when O > length(E) -> [].
render_page(E) ->
        [{obj, [{tstamp, T}, {host, H}, {msg, M}]} || {{_, T}, H, [M|_]} <- E].

count_maps(L) ->
        N = length(lists:filter(fun
                (["map"]) -> true;
                ([["map"]]) -> true;
                (_) -> false
        end, L)),
        {N, length(L) - N}.

render_jobinfo([], _, _, _, _, _) -> [];
render_jobinfo([[Tstamp, [JobPid, NMap, NRed, DoRed, Inputs]]],
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
               {mapi, [NMap - (NMapDone + NMapRun),
                        NMapRun, NMapDone, NMapFail]},
               {redi, [NRed - (NRedDone + NRedRun),
                        NRedRun, NRedDone, NRedFail]},
               {reduce, DoRed},
               {results, lists:map(fun({_, X}) -> list_to_binary(X) end, 
                                lists:flatten(Res))},
               {inputs, lists:map(fun erlang:list_to_binary/1, Inputs)},
               {nodes, lists:map(fun erlang:list_to_binary/1, Nodes)}
        ]}.

