-module(hdfs).
-export([save_to_hdfs/4, get_compliant_name/1]).

get_data_node_link(NameNode, HdfsPath, User) ->
    URL = "http://" ++ NameNode ++ "/webhdfs/v1" ++ HdfsPath ++ "?op=CREATE&user.name=" ++ User,
    Response = httpc:request(put, {URL, [], "text/plain", <<"">>}, [], []),
    {ok, {{_,307,_}, L, []}} = Response,
    Loc = lists:keyfind("location", 1, L),
    element(2, Loc).

-spec save_to_hdfs(string(), string(), string(), string()) -> ok.
save_to_hdfs(NameNode, HdfsPath, User, LocalPath) ->
    DataNodeUrl = get_data_node_link(NameNode, HdfsPath, User),
    Self = self(),
    spawn_link(fun() -> http_client:http_put_conn(LocalPath, DataNodeUrl,
                    Self) end),
    receive S ->
            error_logger:info_msg("Hdfs operation done: ~p~n", [S]),
            S
    end.

-spec get_compliant_name(string()) -> string().
get_compliant_name(Name) ->
    re:replace(Name, ":", "_", [global, {return, list}]).
