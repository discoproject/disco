
-module(ddfs_web).

-include("config.hrl").

-export([op/3]).

op('GET', "/ddfs/new_blob/" ++ BlobName, Req) ->
    BlobK = list_to_integer(disco:get_setting("DDFS_BLOB_REPLICAS")),
    QS = Req:parse_qs(),
    K = case lists:keysearch("replicas", 1, QS) of
            false -> BlobK;
            {value, {_, X}} -> list_to_integer(X)
    end,
    Exc = parse_exclude(lists:keysearch("exclude", 1, QS)),
    Obj = [BlobName, "$", ddfs_util:timestamp()],
    case (ddfs_util:is_valid_name(BlobName) andalso (catch
        gen_server:call(ddfs_master, {new_blob, Obj, K, Exc}))) of

        false ->
            Req:respond({403, [], ["Invalid prefix"]});
        {ok, Urls} when length(Urls) < K ->
            Req:respond({403, [], ["Not enough nodes for replicas"]});
        too_many_replicas ->
            Req:respond({403, [], ["Not enough nodes for replicas"]});
        {ok, Urls} ->
            okjson([list_to_binary(U) || U <- Urls], Req);
        E ->
            error_logger:warning_report({"/ddfs/new_blob failed", E}),
            error(E, Req)
    end;

op('GET', "/ddfs/tags" ++ Prefix0, Req) ->
    Prefix = list_to_binary(
        ddfs_util:replace(string:strip(Prefix0, both, $/), $/, $:)),
    TagMinK = list_to_integer(disco:get_setting("DDFS_TAG_MIN_REPLICAS")),
    case catch gen_server:call(ddfs_master,
            {get_tags, filter}, ?NODEOP_TIMEOUT) of 
        {'EXIT', _} = E ->
            error_logger:warning_report({"/ddfs/tags failed", E}),
            error(E, Req);
        {_OkNodes, Failed, Tags} when length(Failed) < TagMinK ->
            okjson(
                if Prefix =:= <<>> ->
                    Tags;
                true ->
                    [T || T <- Tags, ddfs_util:startswith(T, Prefix)]
            end, Req);
        _ ->
            error({error, too_many_failed_nodes}, Req)
    end;

op('GET', "/ddfs/tag/" ++ Tag0, Req) ->
    Tag = ddfs_util:replace(Tag0, $/, $:),
    case (ddfs_util:is_valid_name(Tag) andalso (catch
        gen_server:call(ddfs_master,
            {tag, get, list_to_binary(Tag)}, ?NODEOP_TIMEOUT))) of

        TagData when is_binary(TagData) ->
            Req:ok({"application/json", [], TagData});
        false ->
            Req:respond({403, [], ["Invalid tag"]});
        deleted ->
            Req:respond({404, [], ["Tag not found"]});
        notfound ->
            Req:respond({404, [], ["Tag not found"]});
        E ->
            error(E, Req)
    end;

op('POST', "/ddfs/tag/" ++ Tag, Req) ->
    tag_update(update, Tag, Req);

op('PUT', "/ddfs/tag/" ++ Tag, Req) ->
    tag_update(put, Tag, Req);

op('DELETE', "/ddfs/tag/" ++ Tag, Req) ->
    tag_update(delete, Tag, Req);

op('GET', Path, Req) ->
    ddfs_get:serve_ddfs_file(Path, Req); 

op(_, _, Req) ->
    Req:not_found().

error({_, timeout}, Req) ->
    Req:respond({503, [], ["Temporary server error. Try again."]});
error({error, E}, Req) when is_atom(E) ->
    Req:respond({500, [], ["Internal server error: ", atom_to_list(E)]});
error(_, Req) ->
    Req:respond({500, [], ["Internal server error"]}).

okjson(Data, Req) ->
    Req:ok({"application/json", [], mochijson2:encode(Data)}).

tag_update(Op, Tag, Req) ->
    case ddfs_util:is_valid_name(Tag) of
        false ->
            Req:respond({403, [], ["Invalid tag"]});
        true ->
            tag_update1(Op, list_to_binary(Tag), Req)
    end.

tag_update1(delete, Tag, Req) ->
    {ok, _} = gen_server:call(ddfs_master,
        {tag, {update, [[list_to_binary(["tag://", Tag])]]}, <<"+deleted">>},
            ?TAG_UPDATE_TIMEOUT),
    (catch gen_server:call(ddfs_master, {tag, die, Tag}, 1)),
    Req:ok({"application/json", [], mochijson2:encode(<<"deleted">>)});

tag_update1(Op, Tag, Req) ->
    case catch mochijson2:decode(Req:recv_body(?MAX_TAG_BODY_SIZE)) of
        {'EXIT', _} ->
            Req:respond({403, [], ["Invalid request body"]});
        Urls ->
            case gen_server:call(ddfs_master,
                    {tag, {Op, Urls}, Tag}, ?TAG_UPDATE_TIMEOUT) of
                {ok, Dst} ->
                    okjson(Dst, Req);
                E ->
                    error(E, Req)
            end
    end.

parse_exclude(false) -> [];
parse_exclude({value, {_, ExcStr}}) ->
    [node_mon:slave_node_safe(Node) || Node <- string:tokens(ExcStr, ",")].

