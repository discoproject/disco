-module(ddfs_tag_util).

-include("ddfs.hrl").
-include("ddfs_tag.hrl").
-include("config.hrl").

-export([check_token/4, encode_tagcontent/1, encode_tagcontent_secure/1,
         decode_tagcontent/1, update_tagcontent/5, delete_tagattrib/3,
         validate_urls/1, validate_value/2]).

-export([make_tagcontent/6]).

-spec check_token(tokentype(), token(), token(), token()) -> tokentype() | false.
check_token(TokenType, Token, ReadToken, WriteToken) ->
    Auth = fun(Tok, CurrentToken) when Tok =:= CurrentToken -> true;
              (internal, _) -> true;
              (_, null) -> true;
              (_, _) -> false
           end,
    WriteAuth = Auth(Token, WriteToken),
    case TokenType of
        read ->
            case Auth(Token, ReadToken) of
                % It is possible that the Token also carries
                % write-privileges, either because the write-token
                % is not set, or because ReadToken =:= WriteToken.
                true when WriteToken =:= null -> write;
                true when WriteToken =:= ReadToken -> write;
                true -> read;

                % We should not allow reads if the
                % WriteToken check passes just because it
                % hasn't been set.
                false when WriteToken =:= null -> false;
                false when WriteAuth =:= true -> write;
                false -> false
            end;
        write when WriteAuth =:= true -> write;
        write -> false
    end.

-spec make_tagcontent(binary(), binary(), token(), token(),
                      [binary()], user_attr()) -> tagcontent().
make_tagcontent(Id, LastModified, ReadToken, WriteToken, Urls, UserAttr) ->
    #tagcontent{id = Id,
                last_modified = LastModified,
                read_token = ReadToken,
                write_token = WriteToken,
                urls = Urls,
                user = UserAttr}.

-spec encode_tagcontent(tagcontent()) -> binary().
encode_tagcontent(D) ->
    list_to_binary(mochijson2:encode({struct,
        [{<<"version">>, 1},
         {<<"id">>, D#tagcontent.id},
         {<<"last-modified">>, D#tagcontent.last_modified},
         {<<"read-token">>, D#tagcontent.read_token},
         {<<"write-token">>, D#tagcontent.write_token},
         {<<"urls">>, D#tagcontent.urls},
         {<<"user-data">>, {struct, D#tagcontent.user}}
        ]})).

-spec encode_tagcontent_secure(tagcontent()) -> binary().
encode_tagcontent_secure(D) ->
    list_to_binary(mochijson2:encode({struct,
        [{<<"version">>, 1},
         {<<"id">>, D#tagcontent.id},
         {<<"last-modified">>, D#tagcontent.last_modified},
         {<<"urls">>, D#tagcontent.urls},
         {<<"user-data">>, {struct, D#tagcontent.user}}
        ]})).

lookup(Key, List) ->
   {_, Value} = lists:keyfind(Key, 1, List),
   Value.
lookup(Key, Default, List) ->
    proplists:get_value(Key, List, Default).

-spec lookup_tagcontent([{_, _}]) -> tagcontent().
lookup_tagcontent(L) ->
    {struct, UserData} = lookup(<<"user-data">>, {struct, []}, L),
    #tagcontent{id = lookup(<<"id">>, L),
                urls = lookup(<<"urls">>, L),
                last_modified = lookup(<<"last-modified">>, L),
                read_token = lookup(<<"read-token">>, null, L),
                write_token = lookup(<<"write-token">>, null, L),
                user = UserData}.

-spec decode_tagcontent(binary()) -> {error, corrupted_json | invalid_object} |
                                     {ok, tagcontent()}.
decode_tagcontent(TagData) ->
    try case mochijson2:decode(TagData) of
            {struct, Body} -> {ok, lookup_tagcontent(Body)};
            _E -> {error, invalid_object}
        end
    catch _ ->   {error, corrupted_json};
          _:_ -> {error, corrupted_json}
    end.

-spec update_tagcontent(tagname(), tagcontent()) -> tagcontent().
update_tagcontent(TagName, Tag) ->
    Tag#tagcontent{id = ddfs_util:pack_objname(TagName, now()),
                   last_modified = ddfs_util:format_timestamp()}.

-spec update_tagcontent(tagname(), attrib(), _, _, token()) ->
                               {error, too_many_attributes | invalid_url_object} |
                               {ok, tagcontent()}.
update_tagcontent(TagName, Field, Value, {ok, Tag}, _Token) ->
    Updated = update_tagcontent(TagName, Tag),
    update_tagcontent(Field, Value, Updated);

% make sure that 'internal' doesn't leak to the tag
update_tagcontent(TagName, Field, Value, Tag, internal) ->
    update_tagcontent(TagName, Field, Value, Tag, null);

update_tagcontent(TagName, Field, Value, _Tag, Token) ->
    New = #tagcontent{read_token = Token,
                      write_token = Token,
                      urls = [],
                      user = []},
    update_tagcontent(TagName, Field, Value, {ok, New}, Token).

-spec update_tagcontent(attrib(), _, tagcontent()) ->
                       {error, too_many_attributes | invalid_url_object} |
                       {ok, tagcontent()}.
update_tagcontent(read_token, Token, Tag) ->
    {ok, Tag#tagcontent{read_token = Token}};

update_tagcontent(write_token, Token, Tag) ->
    {ok, Tag#tagcontent{write_token = Token}};

update_tagcontent(urls, Urls, Tag) ->
    case validate_urls(Urls) of
        true ->
            {ok, Tag#tagcontent{urls = Urls}};
        false ->
            {error, invalid_url_object}
    end;

update_tagcontent({user, _}, _, Tag)
                  when length(Tag#tagcontent.user) >= ?MAX_NUM_TAG_ATTRIBS ->
    {error, too_many_attributes};

update_tagcontent({user, Key}, Attr, Tag) ->
    {ok, Tag#tagcontent{user = lists:keystore(Key,
                                              1,
                                              Tag#tagcontent.user,
                                              {Key, Attr})}}.

-spec delete_tagattrib(tagname(), attrib(), tagcontent()) -> tagcontent().
delete_tagattrib(TagName, read_token, Tag) ->
    update_tagcontent(TagName, Tag#tagcontent{read_token = null});

delete_tagattrib(TagName, write_token, Tag) ->
    update_tagcontent(TagName, Tag#tagcontent{write_token = null});

delete_tagattrib(TagName, urls, Tag) ->
    update_tagcontent(TagName, Tag#tagcontent{urls = []});

delete_tagattrib(TagName, {user, Key}, Tag) ->
    User = lists:keydelete(Key, 1, Tag#tagcontent.user),
    update_tagcontent(TagName, Tag#tagcontent{user = User}).

-spec validate_urls([[_]]) -> boolean().
validate_urls(Urls) ->
    [] =:= (catch lists:flatten([[1 || X <- L, not is_binary(X)] || L <- Urls])).

-spec validate_value(attrib(), _) -> boolean().
validate_value(urls, Value) ->
    validate_urls(Value);
validate_value(read_token, Value) ->
    is_binary(Value);
validate_value(write_token, Value) ->
    is_binary(Value);
validate_value({user, Key}, Value) ->
    is_binary(Key) and is_binary(Value).
