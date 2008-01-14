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


-module(scgi).
-export([receive_scgi_message/1, parse_scgi_message/1]).

receive_scgi_message(Socket) ->
        receive_scgi_message(header, Socket, []).

receive_scgi_message(header, Socket, Msg) ->
        case gen_tcp:recv(Socket, 1) of
                {ok, ":"} -> receive_scgi_message(
                        body, Socket, list_to_integer(
                                lists:reverse(lists:flatten(Msg))));
                {ok, C} -> receive_scgi_message(header, Socket, [C|Msg]);
                _Other -> {error, invalid_scgi_header}
        end;

receive_scgi_message(body, Socket, Length) ->
        case gen_tcp:recv(Socket, Length + 1) of
                {ok, Packet} -> {ok, parse_scgi_message(Packet)};
                _Other -> {error, invalid_scgi_body}
        end.

parse_scgi_message([]) -> [];
parse_scgi_message(Packet) ->
        {ok, A} = regexp:split(Packet, 0),
        parse_scgi_message(A, []).

parse_scgi_message([], Lst) -> Lst;
parse_scgi_message(Packet, Lst) ->
        [Key|A] = Packet,
        case A of
                [] -> Lst;
                [Value|B] -> parse_scgi_message(B, [{Key, Value}|Lst])
        end.
