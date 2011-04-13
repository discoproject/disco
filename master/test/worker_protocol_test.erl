-module(worker_protocol_test).
-export([test/0]).

test() ->
    new_message = worker_protocol:init(),
    {cont, _, {parse_length, <<"FOO">>}} = worker_protocol:parse(<<"FOO 1">>),
    {cont, _, {parse_body, <<"FOO">>, <<"3">>, 10}} = worker_protocol:parse(<<"FOO 3 bar">>),

    {error, invalid_type} = worker_protocol:parse(<<"01234567890abc">>),
    {error, invalid_length} = worker_protocol:parse(<<"FOO 01234567890abc ">>),
    {error, invalid_length} = worker_protocol:parse(<<"FOO BAR ">>),
    {error, message_too_big} = worker_protocol:parse(<<"FOO 1000000000 ">>),
    {error, invalid_body} = worker_protocol:parse(<<"FOO 3 bar ">>),

    {ok, {<<"FOO">>, <<"bar">>}, <<>>, new_message} = worker_protocol:parse(<<"FOO 3 bar\n">>),
    {ok, {<<"FOO">>, <<>>}, <<>>, new_message} = worker_protocol:parse(<<"FOO 0 \n">>),

    Buffer = <<"ABC 3 abc\nDEFG 2 ab\nHIJKL 1 a\ntail">>,
    {ok, {<<"ABC">>, <<"abc">>}, Rest0, _} = worker_protocol:parse(Buffer),
    {ok, {<<"DEFG">>, <<"ab">>}, Rest1, _} = worker_protocol:parse(Rest0),
    {ok, {<<"HIJKL">>, <<"a">>}, <<"tail">>, _} = worker_protocol:parse(Rest1),
    ok.
