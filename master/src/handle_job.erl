
-module(handle_job).
-export([handle/2, job_coordinator/4]).

-define(OK_HEADER, "HTTP/1.1 200 OK\n"
                   "Status: 200 OK\n"
                   "Content-type: text/plain\n\n").

ready({_, "master", ["READY"|Results]}) -> json:encode(Results);
ready(_) -> "Not ready".

new_coordinator(Name, Msg, PostData) ->
        S = self(),
        P = spawn(fun() -> job_coordinator(S, Name, Msg, PostData) end),
        receive
                {P, ok} -> [?OK_HEADER, "job started"]
        after 5000 ->
                [?OK_HEADER, "couldn't start a new job coordinator"]
        end.     

op("new", _Query, PostData) ->
        Msg = netstring:decode_netstring_fd(PostData),
        {value, {_, Name}} = lists:keysearch("name", 1, Msg),
        error_logger:info_report([{"New job", Name}]),
        
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> new_coordinator(Name, Msg, PostData);
                {ok, _Events} -> [?OK_HEADER, 
                                "ERROR: job ", Name, " already exists"]
        end;

op("get_results", Query, none) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> [?OK_HEADER, "ERROR: Unknown job ", Name];
                {ok, Events} -> [?OK_HEADER, ready(lists:last(Events))]
        end.

handle(Socket, Msg) ->
        {value, {_, Script}} = lists:keysearch("PATH_INFO", 1, Msg),
        {value, {_, Query}} = lists:keysearch("QUERY_STRING", 1, Msg),
        {value, {_, CLenStr}} = lists:keysearch("CONTENT_LENGTH", 1, Msg),
        CLen = list_to_integer(CLenStr),
        if CLen > 0 ->
                {ok, PostData} = gen_tcp:recv(Socket, CLen, 0);
        true ->
                PostData = none
        end,
        Op = lists:last(string:tokens(Script, "/")),
        Res = op(Op, httpd:parse_query(Query), PostData),
        gen_tcp:send(Socket, Res).

pref_node([$h, $t, $t, $p, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node([$d, $i, $s, $c, $o, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node(Host) -> Host.

work([{PartID, Input}|Inputs], Mode, Name, Data, N, Max, Res) when N =< Max ->
        PrefNode = pref_node(Input),
        {ok, _} = gen_server:call(disco_server, {new_worker, 
                        {Name, PartID, Mode, {PrefNode, []}, Input, Data}}),
        ets:insert(Res, {{input, PartID}, {Input, Data}}),
        work(Inputs, Mode, Name, Data, N + 1, Max, Res);

work([_|_] = IArg, Mode, Name, Data, N, Max, Res) when N > Max ->
        M = wait_workers(N, Res, Name, Mode),
        work(IArg, Mode, Name, Data, M, Max, Res);

work([], Mode, Name, Data, N, Max, Res) when N > 0 ->
        M = wait_workers(N, Res, Name, Mode),
        work([], Mode, Name, Data, M, Max, Res);

work([], _Mode, _Name, _Data, 0, _Max, _Res) -> ok.

wait_workers(0, _Res, _Name, _Mode) ->
        throw("Nothing to wait");

wait_workers(N, Res, Name, Mode) ->
        M = N - 1,
        receive
                {job_ok, Result, {Node, PartID}} -> 
                        disco_server:event(Name, 
                                "Received results from ~s:~B @ ~s: ~p",
                                        [Mode, PartID, Node, Result], []),
                        ets:insert(Res, {{result, PartID}, Result}), M;

                {data_error, Error, {Node, PartID}} ->
                        handle_data_error(Name, PartID, Mode, Node, Res), N;
                        
                {job_error, Error, {Node, PartID}} ->
                        throw(logged_error);
                        
                {error, Error, {Node, PartID}} ->
                        disco_server:event(Name, 
                                "ERROR: Worker crashed in ~s:~B @ ~s: ~p",
                                        [Mode, PartID, Node, Error], []),
                        throw(logged_error);
                        
                Error ->
                        disco_server:event(Name, 
                                "ERROR: Received an unknown error: ~p",
                                        [Error], []),
                        throw(logged_error)
        end.

check_failure_rate(Name, PartID, Mode, L) ->
        V = case application:get_env(max_failure_rate) of
                undefined -> L > 3;
                {ok, N} -> L > N
        end,
        if V ->
                disco_server:event(Name, 
                        "ERROR: ~s:~B failed ~B times. Aborting job.",
                                [Mode, PartID, L], []),
                throw(logged_error);
        true -> 
                ok
        end.

handle_data_error(Name, PartID, Mode, Node, Res) ->
        ets:insert(Res, {{dataerr, PartID}, Node}),
        {_, ErrNodes} = lists:unzip(ets:lookup(Res, {dataerr, PartID})),
        ok = check_failure_rate(Name, PartID, Mode, length(ErrNodes)),
        [{_, {Input, Data}}] = ets:lookup(Res, {input, PartID}),

        {ok, _} = gen_server:call(disco_server, {new_worker, 
                        {Name, PartID, Mode, {none, ErrNodes}, Input, Data}}).

find_values(Msg) ->
        {value, {_, InputStr}} = lists:keysearch("input", 1, Msg),
        Inputs = string:tokens(InputStr, " "),

        {value, {_, NMapsStr}} = lists:keysearch("nr_maps", 1, Msg),
        NMap = list_to_integer(NMapsStr),
        
        {value, {_, NRedStr}} = lists:keysearch("nr_reduces", 1, Msg),
        NRed = list_to_integer(NRedStr),
                
        case lists:keysearch("reduce", 1, Msg) of 
                false -> {Inputs, NMap, NRed, false};
                _Else -> {Inputs, NMap, NRed, true}
        end.

supervise_work(Inputs, Mode, Name, Msg, MaxN) ->
        Res = ets:new(result_table, [bag]),
        case catch work(Inputs, Mode, Name, Msg, 0, MaxN, Res) of
                ok -> ok;
                logged_error ->
                        disco_server:event(Name, 
                        "ERROR: Job terminated due to the previous errors",
                                [], []),
                        gen_server:call(disco_server, {kill_job, Name}),
                        exit(logged_error);
                Error ->
                        disco_server:event(Name, 
                        "ERROR: Job coordinator failed unexpectedly: ~p", 
                                [Error], []),
                        gen_server:call(disco_server, {kill_job, Name}),
                        exit(unknown_error)
        end, Res.

group_results(Res) -> 
        {ok, lists:foldl(fun
                ({PartID, R}, []) ->
                        [{PartID, [R]}];
                ({PartID, R}, [{PrevID, Lst}|Rest]) when PrevID == PartID ->
                        [{PartID, [R|Lst]}|Rest];
                ({PartID, R}, [{PrevID, _}|_] = Q) when PrevID =/= PartID ->
                        [{PartID, [R]}|Q]
        end, [], lists:keysort(1, 
                 lists:flatten(ets:match(Res, {{result, '_'}, '$1'}))))}.

job_coordinator(Parent, Name, Msg, PostData) ->
        disco_server:event(Name, "Job coordinator starts", [], [start, self()]),
        Parent ! {self(), ok},

        {MapInputs, NMap, NRed, DoReduce} = case catch find_values(Msg) of
                {A, B, C, D} -> {A, B, C, D}; 
                MError -> disco_server:event(Name, 
                        "ERROR: Couldn't parse the job packet: ~p", [MError]),
                        exit(logged_error)
        end,

        disco_server:event(Name, "Starting map phase", [], 
                [map_data, self(), NMap, NRed, DoReduce, MapInputs]),

        EnumMapInputs = lists:zip(
                lists:seq(0, length(MapInputs) - 1), MapInputs),
        error_logger:info_report([{"Inputs", EnumMapInputs}]),
        MapResults = supervise_work(EnumMapInputs, "map", Name, PostData, NMap),

        RedInputs = case catch group_results(MapResults) of
                {ok, RedInp} -> RedInp;
                RError -> disco_server:event(Name, 
                        "ERROR: Couldn't parse map results: ~p", [RError]),
                        exit(logged_error)
        end,
        ets:delete(MapResults),
        disco_server:event(Name, "Map phase done", [], []),

        if DoReduce ->
                disco_server:event(Name, "Starting reduce phase", [],
                        [red_data, RedInputs]),
                RedResults = supervise_work(RedInputs, "reduce", Name, PostData, NRed),
                R = lists:flatten(ets:match(RedResults, {{result, '$1'}, '$2'})),
                disco_server:event(Name, "Reduce phase done", [],
                        [red_done, R]),
                disco_server:event(Name, "READY", [], [R]);
        true ->
                disco_server:event(Name, "READY", [], [MapResults])
        end.
