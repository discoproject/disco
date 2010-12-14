
-module(ddfs_web).

-include("config.hrl").

-export([op/3]).

-spec parse_tag_attribute(string(), atom()) ->
    {string(), ddfs_tag:attrib() | 'all' | 'unknown_attribute'}.
parse_tag_attribute(TagAttrib, DefaultAttrib) ->
    case mochiweb_util:path_split(TagAttrib) of
        {T, ""} -> {T, DefaultAttrib};
        {T, "ddfs:urls"} -> {T, urls};
        {T, "ddfs:read-token"} -> {T, read_token};
        {T, "ddfs:write-token"} -> {T, write_token};
        {T, "ddfs:" ++ _} -> {T, unknown_attribute};
        {T, A} -> {T, {user, list_to_binary(A)}}
    end.

-spec parse_auth_token(module()) -> ddfs_tag:token().
parse_auth_token(Req) ->
    case Req:get_header_value("authorization") of
        undefined ->
            null;
        "Basic " ++ Auth ->
            case base64:decode(Auth) of
                <<"token:", Token/binary>> ->
                    Token;
                _ ->
                    null
            end;
        _ ->
            % Unknown auth, or basic auth that does not follow the spec.
            null
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
            Req:respond({403, [], ["Not enough nodes for replicas."]});
        too_many_replicas ->
            Req:respond({403, [], ["Not enough nodes for replicas."]});
        invalid_name ->
            Req:respond({403, [], ["Invalid prefix."]});
        {ok, Urls} ->
            okjson([list_to_binary(U) || U <- Urls], Req);
        E ->
            error_logger:warning_report({"/ddfs/new_blob failed", E}),
            on_error(E, Req)
    end;

op('GET', "/ddfs/tags" ++ Prefix0, Req) ->
    Prefix = list_to_binary(string:strip(Prefix0, both, $/)),
    case ddfs:tags(ddfs_master, Prefix) of
        {ok, Tags} ->
            okjson(Tags, Req);
        E ->
            on_error(E, Req)
    end;

op('GET', "/ddfs/tag/" ++ TagAttrib, Req) ->
    {Tag, Attrib} = parse_tag_attribute(TagAttrib, all),
    Token = parse_auth_token(Req),
    case Attrib of
        unknown_attribute ->
            Req:respond({404, [], ["Tag attribute not found."]});
        {user, AttribName} when size(AttribName) > ?MAX_TAG_ATTRIB_NAME_SIZE ->
            Req:respond({403, [], ["Attribute name too big."]});
        _ ->
            case ddfs:get_tag(ddfs_master, Tag, Attrib, Token) of
                {ok, TagData} ->
                    Req:ok({"application/json", [], TagData});
                {missing, _} ->
                    Req:respond({404, [], ["Tag not found."]});
                invalid_name ->
                    Req:respond({403, [], ["Invalid tag."]});
                unknown_attribute ->
                    Req:respond({404, [], ["Tag attribute not found."]});
                E ->
                    on_error(E, Req)
            end
    end;

op('POST', "/ddfs/tag/" ++ Tag, Req) ->
    Token = parse_auth_token(Req),
    QS = Req:parse_qs(),
    Opt = if_set("update", QS, [nodup], []),
    tag_update(fun(Urls, _Size) ->
        case is_set("delayed", QS) of
            true ->
                ddfs:update_tag_delayed(ddfs_master, Tag, Urls, Token, Opt);
            false ->
                ddfs:update_tag(ddfs_master, Tag, Urls, Token, Opt)
        end
    end, Req);

op('PUT', "/ddfs/tag/" ++ TagAttrib, Req) ->
    % for backward compatibility, return urls if no attribute is specified
    {Tag, Attrib} = parse_tag_attribute(TagAttrib, urls),
    Token = parse_auth_token(Req),
    case Attrib of
        unknown_attribute ->
            Req:respond({404, [], ["Tag attribute not found."]});
        {user, AttribName} when size(AttribName) > ?MAX_TAG_ATTRIB_NAME_SIZE ->
            Req:respond({403, [], ["Attribute name too big."]});
        _ ->
            Op = fun(Value, Size) ->
                     case Attrib of
                         {user, _} when Size > ?MAX_TAG_ATTRIB_VALUE_SIZE ->
                             Req:respond({403, [], ["Attribute value too big."]});
                         _ ->
                             ddfs:replace_tag(ddfs_master, Tag, Attrib, Value,
                                              Token)
                     end
                 end,
            tag_update(Op, Req)
    end;

op('DELETE', "/ddfs/tag/" ++ TagAttrib, Req) ->
    {Tag, Attrib} = parse_tag_attribute(TagAttrib, all),
    Token = parse_auth_token(Req),
    case Attrib of
        unknown_attribute ->
            Req:respond({404, [], ["Tag attribute not found."]});
        all ->
            case ddfs:delete(ddfs_master, Tag, Token) of
                ok ->
                    Req:ok({"application/json", [], mochijson2:encode(<<"deleted">>)});
                E ->
                    on_error(E, Req)
            end;
        _ ->
            case ddfs:delete_attrib(ddfs_master, Tag, Attrib, Token) of
                ok ->
                    Req:ok({"application/json", [], mochijson2:encode(<<"deleted">>)});
                E ->
                    on_error(E, Req)
            end
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

-spec on_error(_, module()) -> _.
on_error(timeout, Req) ->
    Req:respond({503, [], ["Temporary server error. Try again."]});
on_error({error, timeout}, Req) ->
    Req:respond({503, [], ["Temporary server error. Try again."]});
on_error({error, unauthorized}, Req) ->
    Req:respond({401, [], ["Incorrect or missing token."]});
on_error({error, invalid_name}, Req) ->
    Req:respond({403, [], ["Invalid tag"]});
on_error({error, invalid_url_object}, Req) ->
    Req:respond({403, [], ["Invalid url object"]});
on_error({error, invalid_attribute_value}, Req) ->
    Req:respond({403, [], ["Invalid attribute key or value"]});
on_error({error, too_many_attributes}, Req) ->
    Req:respond({403, [], ["Too many attributes"]});
on_error({error, unknown_attribute}, Req) ->
    Req:respond({404, [], ["Tag attribute not found."]});
on_error({error, E}, Req) when is_atom(E) ->
    Req:respond({500, [], ["Internal server error: ", atom_to_list(E)]});
on_error(E, Req) ->
    Msg = ["Internal server error: ", io_lib:format("~p", [E])],
    Req:respond({500, [], Msg}).

-spec okjson([binary()], module()) -> _.
okjson(Data, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)}).

-spec tag_update(fun(([binary()], non_neg_integer()) -> _), module()) -> _.
tag_update(Fun, Req) ->
    case catch Req:recv_body(?MAX_TAG_BODY_SIZE) of
        {'EXIT', _} ->
            Req:respond({403, [], ["Invalid request."]});
        BinaryPayload ->
            case catch mochijson2:decode(BinaryPayload) of
                {'EXIT', _} ->
                    Req:respond({403, [], ["Invalid request body."]});
                Value ->
                    case Fun(Value, size(BinaryPayload)) of
                        {ok, Dst} ->
                            okjson(Dst, Req);
                        E ->
                            on_error(E, Req)
                    end
            end
    end.

-spec parse_exclude('false' | {'value', {_, string()}}) -> [node()].
parse_exclude(false) -> [];
parse_exclude({value, {_, ExcStr}}) ->
    [disco:node_safe(Host) || Host <- string:tokens(ExcStr, ",")].

