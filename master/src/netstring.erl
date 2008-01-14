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
-export([decode_netstring_fd/1]).

decode_netstring_fd(Msg) ->
        Len = string:sub_word(Msg, 1, 10),
        decode_next_pair(string:substr(Msg, length(Len) + 2, 
                list_to_integer(Len)), []).

decode_next_pair([], Lst) -> Lst;
decode_next_pair(Msg, Lst) ->
        {Msg1, Key} = decode_next_item(Msg),
        {Msg2, Val} = decode_next_item(Msg1),
        decode_next_pair(Msg2, [{Key, Val}|Lst]).

decode_next_item(Msg) ->
        Len = string:sub_word(Msg, 1),
        I = list_to_integer(Len),
        {string:substr(Msg, length(Len) + I + 3), 
                string:substr(Msg, length(Len) + 2, I)}.
