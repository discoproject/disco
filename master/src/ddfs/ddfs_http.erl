-module(ddfs_http).
-export([http_put/3]).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("ddfs.hrl").

-define(CONNECT_TIMEOUT, 60000).
-define(BUFFER_SIZE, 8192).

-spec http_put(path(), nonempty_string(), integer()) ->
                      {'ok', binary()} | {'error', term()}.
http_put(SrcPath, DstUrl, Timeout) ->
    % We use a middleman process to prevent messages received after timeout
    % reaching the original caller.
    P = self(),
    Pid =
        spawn(fun() ->
            process_flag(trap_exit, true),
            S = self(),
            spawn_link(fun() -> http_put_conn(SrcPath, DstUrl, S) end),
            receive
                {'EXIT', _, _} = E -> P ! {S, ddfs_util, {error, E}}, ok;
                E -> P ! {S, ddfs_util, E}, ok
            after Timeout ->
                P ! {S, ddfs_util, {error, timeout}}, ok
            end
        end),
    receive
        {Pid, ddfs_util, Reply} -> Reply
    end.

-spec http_put_conn(path(), nonempty_string(), pid()) -> _.
http_put_conn(SrcPath, DstUrl, Parent) ->
    {_, Addr, Path, _, _} = mochiweb_util:urlsplit(lists:flatten(DstUrl)),
    {Host, Port} = parse_host(Addr),
    Size = content_length(SrcPath, Parent),
    Head = ["PUT ", Path, " HTTP/1.1\r\n",
            "User-Agent: ddfs_http\r\n",
            "Host: ", Host, "\r\n",
            "Content-Length: ", Size, "\r\n\r\n"],
    case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {active, false}],
            ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            ok = gen_tcp:send(Socket, Head),
            {ok, IO} = prim_file:open(SrcPath, [read, raw, binary]),
            ok = send_body(Socket, IO),
            _ = prim_file:close(IO),
            Parent ! read_response(Socket);
        E ->
            die(Parent, E)
    end.

-spec parse_host(nonempty_string()) -> {string(), integer()}.
parse_host(Addr) ->
    case string:tokens(Addr, ":") of
        [Host, Port] -> {Host, list_to_integer(Port)};
        _ -> {Addr, 80}
    end.

-spec content_length(nonempty_string(), pid()) -> string().
content_length(File, Parent) ->
    case prim_file:read_file_info(File) of
        {ok, #file_info{size = S}} -> integer_to_list(S);
        E -> die(Parent, E)
    end.

-spec send_body(port(), file:io_device()) -> 'ok'.
send_body(Socket, IO) ->
    send_body(Socket, IO, prim_file:read(IO, ?BUFFER_SIZE)).
send_body(_, _, eof) -> ok;
send_body(Socket, IO, {ok, Data}) ->
    ok = gen_tcp:send(Socket, Data),
    send_body(Socket, IO).

-spec read_response(port()) ->
    {'error', term()} | {'ok', binary()}.
read_response(Socket) ->
    {ok, RBin} = recv_all(Socket),
    Resp = binary_to_list(RBin),
    gen_tcp:close(Socket),
    H = string:str(Resp, "\r\n\r\n"),
    Body = lists:nthtail(H + 3, Resp),
    case string:tokens(Resp, "\r\n") of
        ["HTTP/1.1 201" ++ _|_] ->
            {ok, list_to_binary(Body)};
        ["HTTP/1.1 " ++ Code|_] ->
            {error, {list_to_integer(string:sub_word(Code, 1, 32)), Body}};
        _ ->
            {error, invalid_response}
    end.

-spec recv_all(port()) -> {'ok', binary()}.
recv_all(Socket) ->
    recv_all(Socket, gen_tcp:recv(Socket, 0), <<>>).

recv_all(_, {error, closed}, Buf) -> {ok, Buf};
recv_all(Socket, {ok, Bin}, Buf) ->
    recv_all(Socket, gen_tcp:recv(Socket, 0), <<Buf/binary, Bin/binary>>).

-spec die(pid(), term()) -> no_return().
die(Parent, E) ->
    Parent ! E,
    exit(error).
