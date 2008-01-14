
-module(handle_ctrl).
-export([handle/2]).

-define(HTTP_HEADER, "HTTP/1.1 200 OK\n"
                     "Status: 200 OK\n"
                     "Content-type: application/json\n\n").

op("save_config_table", Query, Json) ->
        disco_config:save_config_table(Json);

op("load_config_table", Query, Json) -> 
        disco_config:get_config_table().

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
        {ok, Res} = op(Op, Query, Json),
        gen_tcp:send(Socket, [?HTTP_HEADER, Res]).

