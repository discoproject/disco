
-module(ddfs_web).

-include("config.hrl").

-export([op/3]).

-spec op(atom(), string(), module()) -> _.
op('GET', "/ddfs/new_blob/" ++ BlobName, Req) ->
    BlobK = list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS")),
    QS = Req:parse_qs(),
    K = case lists:keysearch("replicas", 1, QS) of
            false -> BlobK;
            {value, {_, X}} -> list_to_integer(X)
    end,
    Exc = parse_exclude(lists:keysearch("exclude", 1, QS)),
    case ddfs:new_blob(ddfs_master, BlobName, K, Exc) of
        {ok, Urls} when length(Urls) < K ->
            Req:respond({403, [], ["Not enough nodes for replicas"]});
        too_many_replicas ->
            Req:respond({403, [], ["Not enough nodes for replicas"]});
        invalid_name ->
            Req:respond({403, [], ["Invalid prefix"]});
        {ok, Urls} ->
            okjson([list_to_binary(U) || U <- Urls], Req);
        E ->
            error_logger:warning_report({"/ddfs/new_blob failed", E}),
            error(E, Req)
    end;

op('GET', "/ddfs/tags" ++ Prefix0, Req) ->
    Prefix = list_to_binary(
        ddfs_util:replace(string:strip(Prefix0, both, $/), $/, $:)),
    case ddfs:tags(ddfs_master, Prefix) of
        {ok, Tags} ->
            okjson(Tags, Req);
        E ->
            error(E, Req)
    end;

op('GET', "/ddfs/tag/" ++ Tag0, Req) ->
    Tag = ddfs_util:replace(Tag0, $/, $:),
    case ddfs:get_tag(ddfs_master, Tag) of
        {ok, TagData} ->
            Req:ok({"application/json", [], TagData});
        invalid_name ->
            Req:respond({403, [], ["Invalid tag"]});
        deleted ->
            Req:respond({404, [], ["Tag not found"]});
        notfound ->
            Req:respond({404, [], ["Tag not found"]});
        E ->
            error(E, Req)
    end;

op('POST', "/ddfs/tag/" ++ Tag, Req) ->
    QS = Req:parse_qs(),
    Opt = if_set("update", QS, [nodup], []),
    tag_update(fun(Urls) ->
        case is_set("delayed", QS) of
            true ->
                ddfs:update_tag_delayed(ddfs_master, Tag, Urls, Opt);
            false ->
                ddfs:update_tag(ddfs_master, Tag, Urls, Opt)
        end
    end, Req);

op('PUT', "/ddfs/tag/" ++ Tag, Req) ->
    tag_update(fun(Urls) -> ddfs:replace_tag(ddfs_master, Tag, Urls) end, Req);

op('DELETE', "/ddfs/tag/" ++ Tag, Req) ->
    case ddfs:delete(ddfs_master, Tag) of
        ok ->
            Req:ok({"application/json", [], mochijson2:encode(<<"deleted">>)});
        E ->
            error(E, Req)
    end;

op('GET', Path, Req) ->
    ddfs_get:serve_ddfs_file(Path, Req);

op(_, _, Req) ->
    Req:not_found().

is_set(Flag, QS) ->
    case lists:keysearch(Flag, 1, QS) of
        {value, {_, [_|_]}} ->
            true;
        _ ->
            false
    end.

if_set(Flag, QS, True, False) ->
    case is_set(Flag, QS) of
        true ->
            True;
        false ->
            False
    end.

-spec error(_, module()) -> _.
error(timeout, Req) ->
    Req:respond({503, [], ["Temporary server error. Try again."]});
error({error, E}, Req) when is_atom(E) ->
    Req:respond({500, [], ["Internal server error: ", atom_to_list(E)]});
error(E, Req) ->
    Msg = ["Internal server error: ", io_lib:format("~p", [E])],
    Req:respond({500, [], Msg}).

-spec okjson([binary()], module()) -> _.
okjson(Data, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)}).

-spec tag_update(fun(([binary()]) -> _), module()) -> _.
tag_update(Fun, Req) ->
    case catch mochijson2:decode(Req:recv_body(?MAX_TAG_BODY_SIZE)) of
        {'EXIT', _} ->
            Req:respond({403, [], ["Invalid request body"]});
        Urls ->
            case Fun(Urls) of
                {ok, Dst} ->
                    okjson(Dst, Req);
                E ->
                    error(E, Req)
            end
    end.

-spec parse_exclude('false' | {'value', {_, string()}}) -> [node()].
parse_exclude(false) -> [];
parse_exclude({value, {_, ExcStr}}) ->
    [disco:node_safe(Host) || Host <- string:tokens(ExcStr, ",")].

