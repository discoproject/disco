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


-module(scgi_server).
-behaviour(gen_server).

-compile([verbose, report_errors, report_warnings, trace, debug_info]).
-define(TCP_OPTIONS, [list, {active, false}, {reuseaddr, true}, {packet, raw}]).

-export([start_link/1, stop/0, handle_request/2, scgi_worker/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-define(ERROR_REDIRECT, "Location: /\r\n\r\n").
-define(ERROR_500, "HTTP/1.0 500 Error\r\n"
                   "Status: 500\r\n\r\n"
                   "500 - Internal Server Error").
-define(ERROR_503, "HTTP/1.1 503 Unavailable\r\n"
                   "Status: 503\r\n\r\n"
                   "503 - Service Unavailable").

% scgi_server external interface

start_link(Port) ->
        error_logger:info_report([{'SCGI SERVER STARTS'}]),
        case gen_server:start_link({local, scgi_server}, 
                        scgi_server, [], []) of
                {ok, Server} -> gen_server:call(scgi_server, {listen, Port}),
                                {ok, Server};
                {error, {already_started, Server}} -> {ok, Server}
        end.

stop() ->
        gen_server:call(scgi_server, stop).

% gen_server callbacks

init(_Args) ->
        process_flag(trap_exit, true),
        {ok, {}}.

handle_call({listen, Port}, _From, State) ->
        case catch gen_tcp:listen(Port, ?TCP_OPTIONS) of
                {ok, LSocket} -> spawn_link(fun() -> scgi_worker(LSocket) end),
                                {reply, ok, LSocket};
                Error -> {stop, {listen_failed, Error}, State}
        end;

handle_call({new_worker, Worker}, _From, LSocket) -> 
        spawn_link(fun() -> scgi_server:scgi_worker(LSocket) end),
        erlang:unlink(Worker),
        {reply, ok, LSocket};

handle_call(stop, _From, LSocket) -> {stop, stop_requested, LSocket}.

handle_info({'EXIT', _Pid, _Reason}, LSocket) -> 
        {noreply, LSocket}.

% scgi stuff

scgi_worker(LSocket) ->
        case gen_tcp:accept(LSocket) of
                {ok, Socket} -> 
                        gen_server:call(scgi_server, {new_worker, self()}),
                        {ok, Msg} = scgi:receive_scgi_message(Socket),
                        scgi_server:handle_request(Socket, Msg),
                        gen_tcp:close(Socket)
        end.

handle_request(Socket, Msg) ->
        case catch dispatch_request(Socket, Msg) of
                ok -> null;
                {'EXIT', {timeout, Error}} ->
                        error_logger:info_report(["Timeout",
                                trunc_io:fprint(Error, 500)]),
                        gen_tcp:send(Socket, ?ERROR_500);
                Error ->
                        error_logger:info_report(["Error",
                                trunc_io:fprint(Error, 500)]),
                        gen_tcp:send(Socket, ?ERROR_500)
        end.

dispatch_request(Socket, Msg) ->
        {value, {_, Path}} = lists:keysearch("SCRIPT_NAME", 1, Msg),
        [_|[N|_]] = string:tokens(Path, "/"),
        Mod = list_to_existing_atom("handle_" ++ N),
        Mod:handle(Socket, Msg).

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
