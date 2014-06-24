-module(ddfs_http).
-export([http_put/3]).
-include("ddfs.hrl").
-include("common_types.hrl").

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
            spawn_link(http_client, http_put_conn, [SrcPath, DstUrl, S]),
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

