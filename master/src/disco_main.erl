-module(disco_main).
-behaviour(supervisor).
-behaviour(application).

-include_lib("kernel/include/inet.hrl").

-compile([verbose, report_errors, report_warnings, trace, debug_info]).
-define(MAX_R, 10).
-define(MAX_T, 60).

-export([init/1, start/2, stop/1]).

write_pid(false) -> ok;
write_pid(PidFile) ->
    case file:write_file(PidFile, os:getpid()) of
        ok -> ok;
        Error ->
            exit(["Could not write PID to ", PidFile, ":", Error])
    end.

start(_Type, _Args) ->
    write_pid(disco:get_setting("DISCO_MASTER_PID")),
    Port = disco:get_setting("DISCO_PORT"),
    supervisor:start_link(disco_main, [list_to_integer(Port)]).

init([Port]) ->
    error_logger:info_report([{"DISCO BOOTS"}]),
    {ok, {{one_for_one, ?MAX_R, ?MAX_T}, [
         {ddfs_master, {ddfs_master, start_link, []},
            permanent, 10, worker, dynamic},
         {event_server, {event_server, start_link, []},
            permanent, 10, worker, dynamic},
         {disco_server, {disco_server, start_link, []},
            permanent, 10, worker, dynamic},
         {mochi_server, {web_server, start, [Port]},
            permanent, 10, worker, dynamic},
         {disco_proxy, {disco_proxy, start, []},
            permanent, 10, worker, dynamic}
        ]
    }}.

stop(_State) ->
    ok.

