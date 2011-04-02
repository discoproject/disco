-module(worker_protocol).
-export([parse/1, parse/2, test/0]).

-define(MAX_MESSAGE_LENGTH, 100 * 1024 * 1024).

parse(Buffer) ->
    parse(Buffer, new_message).

parse(Buffer, new_message) ->
    case head(Buffer) of
        {ok, Type} ->
            parse(Buffer, {parse_length, Type});
        head_missing ->
            {error, invalid_type};
        more_data ->
            {cont, Buffer, new_message}
    end;

parse(Buffer, {parse_length, Type} = State) ->
    TypeLen = size(Type) + 1,
    <<_:TypeLen/binary, Rest/binary>> = Buffer,
    case head(Rest) of
        {ok, LengthStr} ->
            case catch list_to_integer(binary_to_list(LengthStr)) of
                {'EXIT', _} ->
                    {error, invalid_length};
                Length when Length > ?MAX_MESSAGE_LENGTH ->
                    {error, message_too_big};
                Length ->
                    Total = size(Type) + size(LengthStr) + Length + 3,
                    parse(Buffer, {parse_body, Type, LengthStr, Total})
            end;
        head_missing ->
            {error, invalid_length};
        more_data ->
            {cont, Buffer, State}
    end;

parse(Buffer, {parse_body, _Type, _LengthStr, Total} = State)
        when size(Buffer) < Total ->
    {cont, Buffer, State};

parse(Buffer, {parse_body, Type, LengthStr, Total}) ->
    HeadLen = size(Type) + size(LengthStr) + 2,
    BodyLen = Total - HeadLen - 1,
    case Buffer of
        <<_:HeadLen/binary, Body:BodyLen/binary, $\n, Rest/binary>> ->
            {ok, Rest, {Type, Body}};
        _ ->
            {error, invalid_body}
    end.

head(<<Head:1/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:2/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:3/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:4/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:5/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:6/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:7/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:8/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:9/binary, 32, _/binary>>) -> {ok, Head};
head(<<Head:10/binary, 32, _/binary>>) -> {ok, Head};
head(Buffer) when size(Buffer) >= 10 -> head_missing;
head(_) -> more_data.

test() ->
    {cont, <<>>, new_message} = parse(<<>>),
    {cont, _, {parse_length, <<"FOO">>}} = parse(<<"FOO 1">>),
    {cont, _, {parse_body, <<"FOO">>, <<"3">>, 10}} = parse(<<"FOO 3 bar">>),

    {error, invalid_type} = parse(<<"01234567890abc">>),
    {error, invalid_length} = parse(<<"FOO 01234567890abc ">>),
    {error, invalid_length} = parse(<<"FOO BAR ">>),
    {error, message_too_big} = parse(<<"FOO 1000000000 ">>),
    {error, invalid_body} = parse(<<"FOO 3 bar ">>),

    {ok, <<>>, {<<"FOO">>, <<"bar">>}} = parse(<<"FOO 3 bar\n">>),
    {ok, <<>>, {<<"FOO">>, <<>>}} = parse(<<"FOO 0 \n">>),

    Buffer = <<"ABC 3 abc\nDEFG 2 ab\nHIJKL 1 a\ntail">>,
    {ok, Rest0, {<<"ABC">>, <<"abc">>}} = parse(Buffer),
    {ok, Rest1, {<<"DEFG">>, <<"ab">>}} = parse(Rest0),
    {ok, <<"tail">>, {<<"HIJKL">>, <<"a">>}} = parse(Rest1),
    ok.
