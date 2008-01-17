
-module(handle_job).
-export([handle/2, job_coordinator/3]).

-define(OK_HEADER, "HTTP/1.1 200 OK\n"
                   "Status: 200 OK\n"
                   "Content-type: text/plain\n\n").

lastn(E, N) when N < length(E) ->
        [disco_server:format_event(X) || X <- lists:nthtail(length(E) - N, E)];

lastn(E, N) when N >= length(E) ->
        [disco_server:format_event(X) || X <- E].

ready({_, "master", ["READY"|Results]}) -> json:encode(Results);
ready({_, _, [Msg|_]}) -> "Not ready".

new_coordinator(Name, Input, Msg) ->
        P = spawn(fun() -> job_coordinator(self(), Name, Input, Msg) end),
        receive
                {P, ok} -> [?OK_HEADER, "job started"]
        after 5000 ->
                [?OK_HEADER, "couldn't start a new job coordinator"]
        end.     

op("new", Query, Msg) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Msg),
        error_logger:info_report([{"New job", Name}]),
        
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> new_coordinator(Name, Msg);
                {ok, Events} -> [?OK_HEADER, 
                                "ERROR: job ", Name, " already exists"]
        end;

op("status", Query, none) ->
        error_logger:info_report([{"Status", Query}]),
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {value, {_, N}} = lists:keysearch("last", 1, Query),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> [?OK_HEADER, "ERROR: Unknown job ", Name];
                {ok, Events} -> [?OK_HEADER, 
                        json:encode(lastn(Events, list_to_integer(N)))]
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
                {ok, PostData} = gen_tcp:recv(Socket, CLen, 0),
                M = netstring:decode_netstring_fd(PostData);
        true ->
                M = none
        end,
        Op = lists:last(string:tokens(Script, "/")),
        Res = op(Op, httpd:parse_query(Query), M),
        gen_tcp:send(Socket, Res).

pref_node([$h, $t, $t, $p, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node([$d, $i, $s, $c, $o, $:, $/, $/|Uri]) ->
        [Host|_] = string:tokens(Uri, "/"), Host;

pref_node(Host) -> Host.

work([{PartID, Input}|Inputs], Mode, Name, Msg, N, Max, Res) when N <= Max ->
        PrefNode = pref_node(Input),
        {ok, _} = gen_server:call(disco_server, 
                {new_worker, {Name, PartID, Mode, {PrefNode, []}, Input}),
        ets:insert(Res, {{input, PartID}, Input}),
        work(Inputs, Mode, Name, Msg, N + 1, Max, Res);

work([_|_] = IArg, Mode, Name, Msg, N, Max, Res) when N > Max ->
        M = wait_workers(N, Res, Name, Mode),
        work(IArg, Mode, Name, Msg, M, Max, Res);

work([], Mode, Name, Msg, N, Max, Res) ->
        M = wait_workers(N, Res, Name, Mode),
        work([], Mode, Name, Msg, M, Max, Res);

work([], Mode, Name, Msg, 0, Max, Res) -> ok.

wait_workers(0, Res, Name, Mode) ->
        throw("Nothing to wait");

wait_workers(N, Res, Name, Mode) ->
        M = N - 1,
        receive
                {job_ok, Result, {Node, PartID}} -> 
                        disco_server:event(Name, 
                                "Received results from ~s:~w @ ~s: ~w",
                                        [Mode, PartID, Node, Result], []),
                        ets:insert(Res, {{result, PartID}, Result}), M;

                {data_error, Error, {Node, PartID}} ->
                        disco_server:event(Name, 
                                "WARN: Data error in ~s:~w @ ~s: ~w.",
                                        [Mode, PartID, Node, Error], []),
                        handle_data_error(Name, PartID, Mode, Node, Res), N;
                        
                {job_error, Error, {Node, PartID}} ->
                        disco_server:event(Name, 
                                "ERROR: Job error in ~s:~w @ ~s: ~w",
                                        [Mode, PartID, Node, Error], []),
                        throw(logged_error);
                        
                {error, Reason, {Node, PartID}} ->
                        disco_server:event(Name, 
                                "ERROR: General error in ~s:~w @ ~s: ~w",
                                        [Mode, PartID, Node, Error], []),
                        throw(logged_error);
                        
                Error ->
                        disco_server:event(Name, 
                                "ERROR: Unknown error in ~s:~w @ ~s: ~w",
                                        [Mode, PartID, Node, Error], []),
                        throw(logged_error)
        end.

check_failure_rate(Name, PartID, Mode, L) ->
        V = case application:get_env(max_failure_rate) of
                undefined -> L > 3;
                N -> L > N
        end,
        if V ->
                disco_server:event(Name, 
                        "ERROR: ~s:~w failed ~w times. Aborting job.",
                                [Mode, PartID, L], []),
                throw(logged_error);
        true -> 
                ok
        end.

handle_data_error(Name, PartID, Mode, Node, Res) ->
        ets:insert(Res, {{dataerr, PartID}, Node}),
        {_, ErrNodes} = lists:unzip(ets:lookup(Res, {dataerr, PartID})),
        ok = check_failure_rate(Name, PartID, Mode, length(ErrNodes)),
        [{_, Input}] = ets:lookup(Res, {input, PartID}),

        {ok, _} = gen_server:call(disco_server, 
                {new_worker, {Name, PartID, Mode, {none, ErrNodes}, Input}).

find_values(Msg) ->
        {value, {_, InputStr}} = lists:keysearch("input", 1, Msg),
        Inputs = string:tokens(InputStr, " "),

        {value, {_, NMapsStr}} = lists:keysearch("nr_maps", 1, Msg),
        NMap = list_to_integer(NMapsStr),
        
        {value, {_, NRedStr}} = lists:keysearch("nr_reduces", 1, Msg),
        NRed = list_to_integer(NRedStr),
                
        case lists:keysearch("reduce", 1, Msg) of 
                false -> {Inputs, NMap, NRed, false};
                Else -> {Inputs, NMap, NRed, true}
        end.

supervise_work(Inputs, Mode, Name, Msg, MaxN) ->
        Res = ets:new(result_table, [bag]),
        case catch work(Inputs, Mode, Name, Msg, 0, MaxN, Res) of
                ok -> ok;
                logged_error ->
                        disco_server:event(Name, 
                        "ERROR: Job terminated due to the previous errors",
                                [], []),
                        gen_server(disco_server, {kill_job, Name}),
                        exit(logged_error);
                Error ->
                        disco_server:event(Name, 
                        "ERROR: Job coordinator failed unexpectedly: ~w", 
                                [Error], []),
                        gen_server(disco_server, {kill_job, Name}),
                        exit(unknown_error);
        end, Res.

parse_results(Res) -> 
        {ok, lists:foldl(fun
                ({PartID, Res}, []) ->
                        [{PartID, [Res]}];
                ({PartID, Res}, [{PrevID, Lst}|Rest]) when PrevID == PartID ->
                        [{PartID, [Res|Lst]}|Rest];
                ({PartID, Res}, [{PrevID, _}|_] = Q) when PrevID =/= PartID ->
                        [{PartID, [Res]}|Q]
        end, [], lists:keysort(1, 
                 lists:flatten(ets:match(Res, {{result, '_'}, '$1'}))))}.

job_coordinator(Parent, Name, Msg) ->
        disco_server:event(Name, "Job coordinator starts", [], [self()]),
        Parent ! {this(), ok},

        case catch find_values(Msg) of
                {MapInputs, NMap, NRed, DoReduce} ->
                        {MapInputs, NMap, NRed, DoReduce};
                Error -> disco_server:event(Name, 
                        "ERROR: Couldn't parse the job packet: ~w", [Error]),
                        exit(logged_error)
        end

        disco_server:event(Name, "Starting map phase", [], []),
        EnumMapInputs = lists:zip(lists:seq(0, length(Inputs) - 1), Inputs),
        MapResults = supervise_work(EnumMapInputs, "map", Name, Msg, NMap),

        case catch parse_results(MapResults) ->
                {ok, RedInputs} -> RedInputs;
                Error -> disco_server:event(Name, 
                        "ERROR: Couldn't parse map results: ~w", [Error]),
                        exit(logged_error)
        end
        ets:delete(MapResults),
        disco_server:event(Name, "Map phase done", [], []),

        if DoReduce ->
                disco_server:event(Name, "Starting reduce phase", [], []),
                RedResults = supervise_work(RedInputs, "reduce", Name, Msg, NRed),
                R = lists:flatten(ets:match(RedResults, {{result, '$1'}, '$2'})),
                disco_server:event(Name, "Reduce phase done", [], []),
                disco_server:event(Name, "READY", [], [R]);
        true ->
                disco_server:event(Name, "READY", [], [MapResults]);
        end.
