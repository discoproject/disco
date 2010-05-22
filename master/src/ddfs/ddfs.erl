-module(ddfs).

-include("config.hrl").

-export([new_blob/4, tags/2, get_tag/2, update_tag/3, replace_tag/3, get_tag_replicas/2, delete/2]).

new_blob(Host, Blob, Replicas, Exclude) ->
    validate(Blob, fun() ->
        Obj = [Blob, "$", ddfs_util:timestamp()],
        gen_server:call(Host, {new_blob, Obj, Replicas, Exclude})
    end).

tags(Host, Prefix) ->
    case catch gen_server:call(Host, {get_tags, safe}, ?NODEOP_TIMEOUT) of
        {'EXIT', {timeout, _}} ->
            timeout;
        {ok, Tags} ->
            {ok, if Prefix =:= <<>> ->
                Tags;
            true ->
                [T || T <- Tags, ddfs_util:startswith(T, Prefix)]
            end};
        E -> E
    end.

get_tag(Host, Tag) ->
    validate(Tag, fun() ->
        case gen_server:call(Host,
                {tag, get, list_to_binary(Tag)}, ?NODEOP_TIMEOUT) of
            TagData when is_binary(TagData) ->
                {ok, TagData};
            E -> E
        end
    end).

get_tag_replicas(Host, Tag) ->
    tagop(Host, Tag, get_replicas).

update_tag(Host, Tag, Urls) ->
    tagop(Host, Tag, {update, Urls}).

replace_tag(Host, Tag, Urls) ->
    tagop(Host, Tag, {put, Urls}).

delete(Host, Tag) ->
    validate(Tag, fun() ->
        {ok, _} = gen_server:call(Host, {tag, {insert_deleted,
            [list_to_binary(["tag://", Tag])]}, <<"+deleted">>},
                ?TAG_UPDATE_TIMEOUT),
        (catch gen_server:call(ddfs_master,
            {tag, die, list_to_binary(Tag)}, 1)),
        ok
    end).

tagop(Host, Tag, Op) ->
   validate(Tag, fun() ->
        case gen_server:call(Host,
                {tag, Op, list_to_binary(Tag)}, ?TAG_UPDATE_TIMEOUT) of
            {ok, Ret} ->
                {ok, Ret};
            E ->
                E
        end
    end).

validate(Name, Fun) ->
    case ddfs_util:is_valid_name(Name) of
        false ->
            invalid_name;
        true ->
            case catch Fun() of
                {'EXIT', {timeout, _}} ->
                    timeout;
                Ret ->
                    Ret
            end
    end.


