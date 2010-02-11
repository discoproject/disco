
-module(ddfs_web).

-include("config.hrl").

-export([op/3]).

op('GET', "/dfs/blob/" ++ BlobName, Req) ->
    K = case lists:keysearch("replicas", 1, Req:parse_qs()) of
            false -> ?DEFAULT_REPLICAS;
            {value, {_, X}} -> list_to_integer(X)
    end, 
    case (ddfs_util:is_valid_name(BlobName) andalso (catch
        gen_server:call(ddfs_master, {new_blob, BlobName, K}))) of

        false ->
            Req:respond({403, [], ["Invalid prefix"]});
        {ok, Urls} ->
            okjson([list_to_binary(U) || U <- Urls], Req);
        too_many_replicas ->
            Req:respond({403, [], ["Not enough nodes for replicas"]});
        E ->
            error(E, Req)
    end;

op('GET', "/dfs/tags", Req) ->
    case gen_server:call(ddfs_master, {get_tags, filter}, ?NODEOP_TIMEOUT) of 
        {ok, L} ->
            okjson(L, Req);
        E ->
            error(E, Req)
    end;

op('GET', "/dfs/tag/" ++ Tag, Req) ->
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

op('POST', "/dfs/tag/" ++ Tag, Req) ->
    tag_update(update, Tag, Req);

op('PUT', "/dfs/tag/" ++ Tag, Req) ->
    tag_update(put, Tag, Req);

op('DELETE', "/dfs/tag/" ++ Tag, Req) ->
    error_logger:info_report({"DELT", Tag}),
    tag_update(delete, Tag, Req);

op(_, _, Req) ->
    Req:respond({404, [], ["Not found"]}).

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
