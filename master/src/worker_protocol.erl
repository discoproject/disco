-module(worker_protocol).
-export([init/0, parse/1, parse/2]).

-define(MAX_MESSAGE_LENGTH, 100 * 1024 * 1024).

-opaque state() :: new_message
                 | {parse_length, binary()}
                 | {parse_body, binary(), binary(), non_neg_integer()}.

-type parse_result() :: {ok, {binary(), binary()}, binary(), state()}
                      | {error, invalid_type | invalid_length
                                | message_too_big | invalid_body}
                      | {cont, binary(), state()}.

-export_type([state/0]).

-spec init() -> state().
init() ->
    new_message.

-spec parse(binary()) -> parse_result().
parse(Buffer) ->
    parse(Buffer, init()).

-spec parse(binary(), state()) -> parse_result().
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
    TypeLen = byte_size(Type) + 1,
    <<_:TypeLen/binary, Rest/binary>> = Buffer,
    case head(Rest) of
        {ok, LengthStr} ->
            try
                case list_to_integer(binary_to_list(LengthStr)) of
                    Len when Len < 0 ->
                        {error, invalid_length};
                    Len when Len > ?MAX_MESSAGE_LENGTH ->
                        {error, message_too_big};
                    Len ->
                        Total = byte_size(Type) + byte_size(LengthStr) + Len + 3,
                        parse(Buffer, {parse_body, Type, LengthStr, Total})
                end
            catch _:_ -> {error, invalid_length}
            end;
        head_missing ->
            {error, invalid_length};
        more_data ->
            {cont, Buffer, State}
    end;
parse(Buffer, {parse_body, _Type, _LengthStr, Total} = State)
        when byte_size(Buffer) < Total ->
    {cont, Buffer, State};
parse(Buffer, {parse_body, Type, LengthStr, Total}) ->
    HeadLen = byte_size(Type) + byte_size(LengthStr) + 2,
    BodyLen = Total - HeadLen - 1,
    case Buffer of
        <<_:HeadLen/binary, Body:BodyLen/binary, $\n, Rest/binary>> ->
            {ok, {Type, Body}, Rest, new_message};
        _ ->
            {error, invalid_body}
    end.

-spec head(binary()) -> {ok, binary()} | head_missing | more_data.
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
head(Buffer) when byte_size(Buffer) >= 10 -> head_missing;
head(_) -> more_data.
