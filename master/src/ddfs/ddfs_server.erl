-module(ddfs_server).
%-behaviour(gen_server).
%
%-export([start_link/0, stop/0, init/1, handle_call/3, handle_cast/2,
%        handle_info/2, terminate/2, code_change/3]).
%
%start_link() ->
%    error_logger:info_report([{"Event server starts"}]),
%    case gen_server:start_link({local, event_server}, event_server, [], []) of
%            {ok, Server} -> {ok, Server};
%            {error, {already_started, Server}} -> {ok, Server}
%    end.
%
%stop() ->
%        gen_server:call(event_server, stop).
%
%init(_Args) ->
%        ets:new(event_files, [named_table]),
%        {ok, {dict:new(), dict:new()}}.
