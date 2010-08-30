-module(ddfs).

-include("config.hrl").

-export([new_blob/4, tags/2, get_tag/2, update_tag/3, update_tag/4,
         update_tag_delayed/3, update_tag_delayed/4,
         replace_tag/3, get_tag_replicas/2, delete/2]).

-spec new_blob(node(), string(), non_neg_integer(), [node()]) ->
    'invalid_name' | 'too_many_replicas' | {'ok', [string()]} | _.
new_blob(Host, Blob, Replicas, Exclude) ->
    validate(Blob, fun() ->
        Obj = [Blob, "$", ddfs_util:timestamp()],
        gen_server:call(Host, {new_blob, Obj, Replicas, Exclude})
    end).

-spec tags(node(), binary()) -> 'timeout' | {'ok', [binary()]}.
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

-spec get_tag(node(), string()) ->
    'invalid_name' | {'ok', binary()} | {'error', _}.
get_tag(Host, Tag) ->
    validate(Tag, fun() ->
        case gen_server:call(Host,
                {tag, get, list_to_binary(Tag)}, ?NODEOP_TIMEOUT) of
            TagData when is_binary(TagData) ->
                {ok, TagData};
            E -> E
        end
    end).

-spec get_tag_replicas(node(), string()) -> _.
get_tag_replicas(Host, Tag) ->
    tagop(Host, Tag, get_replicas).

-spec update_tag(node(), string(), [binary()]) -> _.
update_tag(Host, Tag, Urls) ->
    update_tag(Host, Tag, Urls, []).

-spec update_tag(node(), string(), [binary()], [term()]) -> _.
update_tag(Host, Tag, Urls, Opt) ->
    tagop(Host, Tag, {update, Urls, Opt}).

-spec update_tag_delayed(node(), string(), [binary()]) -> _.
update_tag_delayed(Host, Tag, Urls) ->
    update_tag_delayed(Host, Tag, Urls, []).

-spec update_tag_delayed(node(), string(), [binary()], [term()]) -> _.
update_tag_delayed(Host, Tag, Urls, Opt) ->
    tagop(Host, Tag, {delayed_update, Urls, Opt}).

-spec replace_tag(node(), string(), [binary()]) -> _.
replace_tag(Host, Tag, Urls) ->
    tagop(Host, Tag, {put, Urls}).

-spec delete(node(), string()) -> _.
delete(Host, Tag) ->
    Fun =
        fun() ->
            BTag = list_to_binary(Tag),
            Urls = [[<<"tag://", BTag/binary>>]],
            Msg = {tag, {update, Urls, [nodup]}, <<"+deleted">>},
            {ok, _} = gen_server:call(Host, Msg, ?TAG_UPDATE_TIMEOUT),
            (catch gen_server:call(ddfs_master, {tag, die, BTag}, 1)),
            ok
        end,
    validate(Tag, Fun).

-spec tagop(node(), string(), _) -> _.
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

-spec validate(string(), fun(()-> T)) -> T.
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


