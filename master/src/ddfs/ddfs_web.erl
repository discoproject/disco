
-module(ddfs_web).

-include("config.hrl").

-export([op/3]).

-spec parse_tag_attribute(string(), atom()) -> {string(), atom() | string()}.
parse_tag_attribute(TagAttrib, DefaultAttrib) ->
    case mochiweb_util:path_split(TagAttrib) of
        {T, ""} -> {T, DefaultAttrib};
        {T, "ddfs:urls"} -> {T, urls};
        {T, "ddfs:read-token"} -> {T, read_token};
        {T, "ddfs:write-token"} -> {T, write_token};
        {T, A} -> {T, {user, list_to_binary(A)}}
    end.

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
    Prefix = list_to_binary(string:strip(Prefix0, both, $/)),
    case ddfs:tags(ddfs_master, Prefix) of
        {ok, Tags} ->
            okjson(Tags, Req);
        E ->
            error(E, Req)
    end;

op('GET', "/ddfs/tag/" ++ TagAttrib, Req) ->
    {Tag, Attrib} = parse_tag_attribute(TagAttrib, all),
    case ddfs:get_tag(ddfs_master, Tag, Attrib) of
        {ok, TagData} ->
            Req:ok({"application/json", [], TagData});
        invalid_name ->
            Req:respond({403, [], ["Invalid tag"]});
        deleted ->
            Req:respond({404, [], ["Tag not found"]});
        notfound ->
            Req:respond({404, [], ["Tag not found"]});
        unknown_attribute ->
            Req:respond({404, [], ["Tag attribute not found"]});
        E ->
            error(E, Req)
    end;

op('POST', "/ddfs/tag/" ++ Tag, Req) ->
    tag_update(fun(Urls) ->
        case lists:keysearch("delayed", 1, Req:parse_qs()) of
            false ->
                ddfs:update_tag(ddfs_master, Tag, Urls);
            _ ->
                ddfs:update_tag_delayed(ddfs_master, Tag, Urls)
        end
    end, Req);

op('PUT', "/ddfs/tag/" ++ TagAttrib, Req) ->
    % for backward compatibility, return urls if no attribute is specified
    {Tag, Attrib} = parse_tag_attribute(TagAttrib, urls),
    tag_update(fun(Value) ->
                   ddfs:replace_tag(ddfs_master, Tag, Attrib, Value)
               end, Req);

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

-spec error(_, module()) -> _.
error(timeout, Req) ->
    Req:respond({503, [], ["Temporary server error. Try again."]});
error({error, E}, Req) when is_atom(E) ->
    Req:respond({500, [], ["Internal server error: ", atom_to_list(E)]});
error(_, Req) ->
    Req:respond({500, [], ["Internal server error"]}).

-spec okjson([binary()], module()) -> _.
okjson(Data, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)}).

-spec tag_update(fun(([binary()]) -> _), module()) -> _.
tag_update(Fun, Req) ->
    case catch mochijson2:decode(Req:recv_body(?MAX_TAG_BODY_SIZE)) of
        {'EXIT', _} ->
            Req:respond({403, [], ["Invalid request body"]});
        Value ->
            case Fun(Value) of
                {ok, Dst} ->
                    okjson(Dst, Req);
                E ->
                    error(E, Req)
            end
    end.

-spec parse_exclude('false' | {'value', {_, string()}}) -> [node()].
parse_exclude(false) -> [];
parse_exclude({value, {_, ExcStr}}) ->
    [node_mon:slave_node_safe(Node) || Node <- string:tokens(ExcStr, ",")].

