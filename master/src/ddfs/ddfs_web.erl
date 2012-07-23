
-module(ddfs_web).

-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").

-export([op/3]).

-type json() :: binary() | [binary()] | {'struct', [{binary(), json()}]}.

-spec parse_tag_attribute(string(), atom()) ->
    {nonempty_string(), attrib() | 'all' | 'unknown_attribute'}.
parse_tag_attribute(TagAttrib, DefaultAttrib) ->
    case mochiweb_util:path_split(TagAttrib) of
        {T, ""} -> {T, DefaultAttrib};
        {T, "ddfs:urls"} -> {T, urls};
        {T, "ddfs:read-token"} -> {T, read_token};
        {T, "ddfs:write-token"} -> {T, write_token};
        {T, "ddfs:" ++ _} -> {T, unknown_attribute};
        {T, A} -> {T, {user, list_to_binary(A)}}
    end.

-spec parse_auth_token(module()) -> token().
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
op('POST', "/ddfs/ctrl/hosted_tags", Req) ->
    Fun =
        fun(BinHost, _Size) ->
            Host = binary_to_list(BinHost),
            case ddfs_master:get_hosted_tags(Host) of
                {ok, Tags} ->
                    okjson(Tags, Req);
                E ->
                    lager:warning("/ddfs/ctrl/hosted_tags failed for ~p", [Host]),
                    on_error(E, Req)
            end
        end,
    process_payload(Fun, Req);

op('GET', "/ddfs/ctrl/gc_stats", Req) ->
    case ddfs_master:gc_stats() of
        {ok, none} ->
            okjson(<<"">>, Req);
        {ok, {{{{TKF, TKB},{TDF, TDB}}, {{BKF, BKB}, {BDF,BDB}}}, TStamp}} ->
            When = disco_util:format_timestamp(TStamp),
            Resp = {struct, [{<<"timestamp">>, When},
                             {<<"stats">>, [{<<"Tags kept">>, [TKF, TKB]},
                                            {<<"Tags deleted">>, [TDF, TDB]},
                                            {<<"Blobs kept">>, [BKF, BKB]},
                                            {<<"Blobs deleted">>, [BDF, BDB]}]}]},
            okjson(Resp, Req);
        E ->
            on_error(E, Req)
    end;

op('GET', "/ddfs/ctrl/gc_status", Req) ->
    case ddfs_gc:gc_request(status) of
        {ok, not_running} ->
            okjson(<<"">>, Req);
        {ok, init_wait} ->
            okjson(<<"GC is waiting for the cluster to stabilize after startup.">>, Req);
        {ok, Phase} ->
            okjson(gc_phase_msg(Phase), Req);
        E ->
            on_error(E, Req)
    end;

op('GET', "/ddfs/ctrl/gc_start", Req) ->
    case ddfs_gc:gc_request(start) of
        {ok, init_wait} ->
            okjson(<<"GC is waiting">>, Req);
        ok ->
            okjson(<<"GC has started">>, Req);
        E ->
            on_error(E, Req)
    end;

op('GET', "/ddfs/ctrl/safe_gc_blacklist", Req) ->
    case ddfs_master:safe_gc_blacklist() of
        {ok, Nodes} ->
            Resp = [list_to_binary(disco:host(N)) || N <- Nodes],
            okjson(Resp, Req);
        E ->
            on_error(E, Req)
    end;

op('GET', "/ddfs/new_blob/" ++ BlobName, Req) ->
    BlobK = list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS")),
    QS = Req:parse_qs(),
    K = case lists:keyfind("replicas", 1, QS) of
            false -> BlobK;
            {_, X} -> list_to_integer(X)
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
            lager:warning("/ddfs/new_blob failed: ~p", [E]),
            on_error(E, Req)
    end;

op('GET', "/ddfs/tags" ++ Prefix0, Req) ->
    Prefix = list_to_binary(string:strip(Prefix0, both, $/)),
    try case ddfs:tags(ddfs_master, Prefix) of
            {ok, Tags} -> okjson(Tags, Req);
            E -> on_error(E, Req)
        end
    catch K:V -> on_error({K,V}, Req)
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
    process_payload(
      fun(Urls, _Size) ->
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
            process_payload(Op, Req)
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
    DdfsRoot = disco:get_setting("DDFS_DATA"),
    ddfs_get:serve_ddfs_file(DdfsRoot, Path, Req);
op('HEAD', Path, Req) ->
    DdfsRoot = disco:get_setting("DDFS_DATA"),
    ddfs_get:serve_ddfs_file(DdfsRoot, Path, Req);

op(_, _, Req) ->
    Req:not_found().

is_set(Flag, QS) ->
    case lists:keyfind(Flag, 1, QS) of
        {_, [_|_]} ->
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

gc_phase_msg(start) ->
    <<"GC is initializing (phase start).">>;
gc_phase_msg(build_map) ->
    <<"GC is scanning DDFS (phase build_map).">>;
gc_phase_msg(map_wait) ->
    <<"GC is scanning DDFS (phase map_wait).">>;
gc_phase_msg(gc) ->
    <<"GC is performing garbage collection (phase gc).">>;
gc_phase_msg(rr_blobs) ->
    <<"GC is re-replicating blobs (phase rr_blobs).">>;
gc_phase_msg(rr_blobs_wait) ->
    <<"GC is re-replicating blobs (phase rr_blobs_wait).">>;
gc_phase_msg(rr_tags) ->
    <<"GC is re-replicating tags (phase rr_tags).">>.

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
on_error({error, unknown_host}, Req) ->
    Req:respond({404, [], ["Unknown host."]});
on_error({error, E}, Req) when is_atom(E) ->
    Req:respond({500, [], ["Internal server error: ", atom_to_list(E)]});
on_error(E, Req) ->
    Msg = ["Internal server error: ", io_lib:format("~p", [E])],
    Req:respond({500, [], Msg}).

-spec okjson(json(), module()) -> _.
okjson(Data, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)}).

-spec process_payload(fun(([binary()], non_neg_integer()) -> _), module()) -> _.
process_payload(Fun, Req) ->
    try  BinaryPayload = Req:recv_body(?MAX_TAG_BODY_SIZE),
         Payload = try mochijson2:decode(BinaryPayload)
                   catch _:_ -> invalid end,
         case Payload of
             invalid ->
                 Req:respond({403, [], ["Invalid request body."]});
             Value ->
                 case Fun(Value, size(BinaryPayload)) of
                     {ok, Dst} -> okjson(Dst, Req);
                     E -> on_error(E, Req)
                 end
         end
    catch _:_ -> Req:respond({403, [], ["Invalid request."]})
    end.

-spec parse_exclude('false' | {'value', {_, string()}}) -> [node()].
parse_exclude(false) -> [];
parse_exclude({value, {_, ExcStr}}) ->
    [disco:slave_safe(Host) || Host <- string:tokens(ExcStr, ",")].
