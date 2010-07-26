-module(ddfs_tag_test).

-export([test/0]).

-include_lib("triq/include/triq.hrl").

% Workaround for Issue 161.
safe_binary() ->
    ?LET(X, list(char()), list_to_binary(X)).

token() ->
    oneof([null, safe_binary()]).

tokentype() ->
    oneof([read, write]).

user_attr() ->
    list({safe_binary(), safe_binary()}).

tagcontent() ->
    ddfs_tag:make_tagcontent(safe_binary(), % id
                             safe_binary(), % last_modified
                             token(), % read_token
                             token(), % write_token
                             list(safe_binary()), % urls
                             user_attr()). % user

tag_equiv(Input, ParseOutput) ->
    {ok, Output} = ParseOutput,
    case Input == Output of
        true -> ok;
        false -> io:format(" Input=~p~n Output=~p~n", [Input, Output])
    end,
    Input == Output.

% Sanity check the tag parser and generator.
parse_gen() ->
    ?FORALL(T, tagcontent(),
            tag_equiv(T, ddfs_tag:parse_tagcontent(ddfs_tag:make_tagdata(T)))).

% Ensure that tag date returned as a result of API calls contain the
% appropriate tokens.

read_token_check(T) ->
    D = ddfs_tag:make_api_tagdata(T, read),
    {struct, J} = mochijson2:decode(D),
    (false =/= lists:keysearch(<<"read-token">>, 1, J))
        and (false =:= lists:keysearch(<<"write-token">>, 1, J)).
write_token_check(T) ->
    D = ddfs_tag:make_api_tagdata(T, write),
    {struct, J} = mochijson2:decode(D),
    (false =/= lists:keysearch(<<"read-token">>, 1, J))
        and (false =/= lists:keysearch(<<"write-token">>, 1, J)).

api_token_test() ->
    % This does not really need to be a FORALL, just a single instance
    % would do.  It would be nice for triq to support this.  Using
    % eunit here causes two problems:
    % (i) eunit and triq both define LET
    % (ii) there is no simple api to get a single value from a triq
    %      generator for an eunit test.
    ?FORALL(T, tagcontent(),
            read_token_check(T) and write_token_check(T)).

prop_test() ->
    io:fwrite("[api_token_test]~n", []),
    triq:check(api_token_test()),
    io:fwrite("[parse_gen]~n", []),
    triq:check(parse_gen()).

% Non-property tests.

check_token_tests() ->
    % default; backward-compatibility
    write = ddfs_tag:check_token(read, null, null, null),
    write = ddfs_tag:check_token(write, null, null, null),
    write = ddfs_tag:check_token(read, <<"token">>, null, null),
    write = ddfs_tag:check_token(write, <<"token">>, null, null),
    % read tests
    read = ddfs_tag:check_token(read, null, null, <<"write-token">>),
    false = ddfs_tag:check_token(read, null, <<"read-token">>, <<"write-token">>),
    read = ddfs_tag:check_token(read, <<"read-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag:check_token(read, <<"write-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag:check_token(read, <<"read-token">>, <<"read-token">>, null),
    % write tests
    write = ddfs_tag:check_token(write, null, <<"read-token">>, null),
    false = ddfs_tag:check_token(write, null, <<"read-token">>, <<"write-token">>),
    false = ddfs_tag:check_token(write, <<"read-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag:check_token(read, <<"write-token">>, <<"read-token">>, <<"write-token">>),
    write = ddfs_tag:check_token(write, <<"read-token">>, <<"read-token">>, null).

test() ->
    io:fwrite("[check_token_tests]~n", []),
    check_token_tests().
