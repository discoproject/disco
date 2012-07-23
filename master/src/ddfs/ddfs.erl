-module(ddfs).

-include("gs_util.hrl").
-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").

-export([new_blob/4, tags/2, get_tag/4, update_tag/5, update_tag_delayed/5,
         replace_tag/5, delete/3, delete_attrib/4]).

% dummy
-type state() :: none.

-spec new_blob(server(), string(), non_neg_integer(), [node()])
              -> invalid_name | too_many_replicas | {ok, [string()]} | _.
new_blob(Server, Blob, Replicas, Exclude) ->
    validate(Blob, fun() ->
                       Obj = lists:flatten([Blob, "$", ddfs_util:timestamp()]),
                       ddfs_master:new_blob(Server, Obj, Replicas, Exclude)
                   end).

-spec tags(server(), binary()) -> {ok, [binary()]} | {error, term()}.
tags(Server, Prefix) ->
    case ddfs_master:get_tags(Server, safe, ?NODEOP_TIMEOUT) of
        {ok, Tags} ->
            {ok, if Prefix =:= <<>> -> Tags;
                    true -> [T || T <- Tags, ddfs_util:startswith(T, Prefix)]
                 end};
        E -> E
    end.

-spec get_tag(server(), nonempty_string(), atom() | string(), token() | internal)
             -> invalid_name | {missing, _} | unknown_attribute | {ok, binary()} | {error, _}.
get_tag(Server, Tag, Attrib, Token) ->
    tagop(Server, Tag, {get, Attrib, Token}, ?NODEOP_TIMEOUT).

-spec update_tag(server(), nonempty_string(), [[binary()]], token(), [term()]) -> _.
update_tag(Server, Tag, Urls, Token, Opt) ->
    tagop(Server, Tag, {update, Urls, Token, Opt}).

-spec update_tag_delayed(server(), nonempty_string(), [[binary()]],
                         token(), [term()]) -> _.
update_tag_delayed(Server, Tag, Urls, Token, Opt) ->
    tagop(Server, Tag, {delayed_update, Urls, Token, Opt}).

-spec replace_tag(server(), nonempty_string(),
                  attrib(), [binary()] | [[binary()]], token()) -> _.
replace_tag(Server, Tag, Field, Value, Token) ->
    tagop(Server, Tag, {put, Field, Value, Token}).

-spec delete_attrib(server(), nonempty_string(), attrib(), token()) -> _.
delete_attrib(Server, Tag, Field, Token) ->
    tagop(Server, Tag, {delete_attrib, Field, Token}).

-spec delete(server(), nonempty_string(), token() | internal) -> term().
delete(Server, Tag, Token) ->
    tagop(Server, Tag, {delete, Token}).

-spec tagop(server(), nonempty_string(),
            {delete, _}
            | {delete_attrib, _, _}
            | {delayed_update, _, _, _}
            | {update, _, _, _}
            | {put, _, _, _}) -> term().
tagop(Server, Tag, Op) ->
    tagop(Server, Tag, Op, ?TAG_UPDATE_TIMEOUT).
tagop(Server, Tag, Op, Timeout) ->
    validate(Tag, fun() ->
                          gen_server:call(Server,
                                          {tag, Op, list_to_binary(Tag)}, Timeout)
                  end).

-spec validate(nonempty_string(), fun(()-> T)) -> T.
validate(Name, Fun) ->
    case ddfs_util:is_valid_name(Name) of
        false ->
            {error, invalid_name};
        true ->
            try Fun()
            catch exit:{timeout,_} -> {error,timeout};
                  K:V -> {error, {K,V}}
            end
    end.
