
-module(handle_job).
-export([handle/2]).

-define(OK_HEADER, "HTTP/1.1 200 OK\n"
                   "Status: 200 OK\n"
                   "Content-type: text/plain\n\n").

op("new", Msg) ->
        error_logger:info_report([{"New job"}]),
        {value, Name} = lists:keysearch("name", 1, Msg),
        {value, Input} = lists:keysearch("input", 1, Msg),
        
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> spawn(fun(Msg) -> 
                                job_coordinator(Name, Input, Msg) end),
                                [?OK_HEADER, "job started"];
                {ok, Events} -> [?OK_HEADER, 
                                "ERROR: job ", Name, " already exists"];
        end;

op("status", Msg) ->
        {value, Name} = lists:keysearch("name", 1, Msg),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> [?OK_HEADER, "ERROR: Unknown job ", Name];
                {ok, Events} -> {t, [M|_]} = lists:last(Events),
                                [?OK_HEADER, M]
        end;

op("get_results", Msg) ->
        {value, Name} = lists:keysearch("name", 1, Msg),
        case gen_server:call(disco_server, {get_job_events, Name}) of
                {ok, []} -> [?OK_HEADER, "ERROR: Unknown job ", Name];
                {ok, Events} -> {t, [_|Results]} = lists:last(Events),
                                [?OK_HEADER, Results]
        end;

handle(Socket, Msg) ->
        {value, {_, Script}} = lists:keysearch("SCRIPT_NAME", 1, Msg),
        {value, {_, CLen}} = lists:keysearch("CONTENT_LENGTH", 1, Msg),
        {ok, PostData} = gen_tcp:recv(Socket, list_to_integer(CLen), 0),
        M = netstring:decode_netstring_fd(PostData),
        Op = lists:last(string:tokens(Script, "/")),
        gen_tcp:send(Socket, op(Op, M)).

job_coordinator(Name, Input, Msg) ->
        gen_server:call(disco_server, {add_job_event, Name,
                ["Job coordinator starts", self()]),





        
        
        
        
