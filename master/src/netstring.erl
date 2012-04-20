% Copyright (c) 2007 Ville H. Tuulos
%
% Permission is hereby granted, free of charge, to any person obtaining a copy
% of this software and associated documentation files (the "Software"), to deal
% in the Software without restriction, including without limitation the rights
% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
% copies of the Software, and to permit persons to whom the Software is
% furnished to do so, subject to the following conditions:
%
% The above copyright notice and this permission notice shall be included in
% all copies or substantial portions of the Software.
%
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
% THE SOFTWARE.

-module(netstring).

-export([encode_netstring_fd/1, decode_netstring_fd/1, decode_netstring_fdx/1]).

-type key() :: binary().
-type value() :: binary().
-type kvtable() :: [{key(), value()}].

-spec encode_netstring_fd(kvtable()) -> binary().
encode_netstring_fd(Lst) ->
    B = << <<(encode_item(K))/binary, " ", 
         (encode_item(V))/binary, "\n">> || {K, V} <- Lst>>,
    S = list_to_binary(integer_to_list(byte_size(B))),
    <<S/binary, "\n", B/binary>>.  

-spec encode_item(key() | value() | [value()]) -> binary().
encode_item(E) when is_list(E) -> encode_item(list_to_binary(E));
encode_item(E) ->
    S = list_to_binary(integer_to_list(byte_size(E))),
    <<S/binary, " ", E/binary>>.


-spec decode_netstring_fd(binary()) -> kvtable().
decode_netstring_fd(Msg) ->
    {L, _} = decode_netstring_fdx(Msg), L.

-spec decode_netstring_fdx(binary()) -> {kvtable(), binary()}.
decode_netstring_fdx(Msg) ->
    Len = bin_sub_word(Msg, <<>>, <<"\n">>),
    P1 = byte_size(Len) + 1,
    P2 = list_to_integer(binary_to_list(Len)),
    <<_:P1/binary, Msg0:P2/binary, Rest/binary>> = Msg,
    {decode_next_pair(Msg0, []), Rest}.

-spec decode_next_pair(binary(), kvtable()) -> kvtable().
decode_next_pair(<<>>, Lst) -> Lst;
decode_next_pair(Msg, Lst) ->
    {Msg1, Key} = decode_next_item(Msg),
    {Msg2, Val} = decode_next_item(Msg1),
    decode_next_pair(Msg2, [{Key, Val}|Lst]).

-spec decode_next_item(binary()) -> {binary(), binary()}.
decode_next_item(Msg) ->
    Len = bin_sub_word(Msg, <<>>, <<" ">>),
    P = byte_size(Len) + 1,
    I = list_to_integer(binary_to_list(Len)),
    <<_:P/binary, X:I/binary, _:1/binary, Rest/binary>> = Msg,
    {Rest, X}.

-spec bin_sub_word(binary(), binary(), binary()) -> binary().
bin_sub_word(<<C:1/binary, _/binary>>, Buf, Delim) when C == Delim -> Buf;
bin_sub_word(<<C:1/binary, Rest/binary>>, Buf, Delim) ->
    bin_sub_word(Rest, <<Buf/binary, C/binary>>, Delim).
    
