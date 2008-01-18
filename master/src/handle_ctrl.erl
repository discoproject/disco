
-module(handle_ctrl).
-export([handle/2]).

-define(HTTP_HEADER, "HTTP/1.1 200 OK\n"
                     "Status: 200 OK\n"
                     "Content-type: text/plain\n\n").

op("save_config_table", Query, Json) ->
        disco_config:save_config_table(Json);

op("load_config_table", Query, Json) -> 
        disco_config:get_config_table();

op("joblist", Query, Json) ->
        {ok, Lst} = gen_server:call(disco_server, get_jobnames),
        Nu = now(),
        TLst = lists:map(fun([J, T, P]) ->
                {round(timer:now_diff(Nu, T) / 1000000),
                        is_process_alive(P), list_to_binary(J)}
        end, Lst),
        {ok, lists:keysort(1, TLst)};

op("jobinfo", Query, Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {ok, {MapNfo, RedNfo, EndNfo}}
                = gen_server:call(disco_server, {get_jobinfo, Name}),

        error_logger:info_report([{"KE", MapNfo, RedNfo, EndNfo}]),
        {ok, render_mapnfo(MapNfo)};

op("jobevents", Query, Json) ->
        {value, {_, Name}} = lists:keysearch("name", 1, Query),
        {value, {_, N}} = lists:keysearch("last", 1, Query),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> {ok, []};
                {ok, Events} -> {ok, lastn(Events, list_to_integer(N))}
        end;

op("nodeinfo", Query, Json) ->
        {ok, {Available, Active}} = 
                gen_server:call(disco_server, {get_nodeinfo, all}),
        ActiveB = lists:map(fun([Node, JobName]) ->
                {obj, [{node, list_to_binary(Node)},
                       {jobname, list_to_binary(JobName)}]}
        end, Active),
        error_logger:info_report([{"KE", Available, ActiveB}]),
        {ok, {obj, [{available, Available}, {active, ActiveB}]}}.

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

lastn(E, -1) ->
        [disco_server:format_event(X) || X <- E];
lastn(E, N) when N >= length(E) ->
        lastn(E, -1);
lastn(E, N) when N < length(E) ->
        [disco_server:format_event(X) || X <- lists:nthtail(length(E) - N, E)].

render_mapnfo([]) -> [];
render_mapnfo([[Tstamp, [JobPid, NMap, NRed, DoRed, Inputs]]]) ->
        T = disco_server:format_timestamp(Tstamp),
        A = is_process_alive(JobPid),
        {obj, [{timestamp, list_to_binary(T)}, 
               {active, A},
               {nmap, NMap},
               {nred, NRed},
               {reduce, DoRed},
               {inputs, lists:map(fun erlang:list_to_binary/1, Inputs)}]}.

