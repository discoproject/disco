-module(disco_main).
-behaviour(supervisor).
-behaviour(application).

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
    SCGIPort = list_to_integer(disco:get_setting("DISCO_SCGI_PORT")),
    supervisor:start_link(disco_main, [SCGIPort]).

init([SCGIPort]) ->
    error_logger:info_report([{"DISCO BOOTS"}]),
    {ok, {{one_for_one, ?MAX_R, ?MAX_T},
         [{event_server, {event_server, start_link, []},
            permanent, 10, worker, dynamic},
         {disco_server, {disco_server, start_link, []},
            permanent, 10, worker, dynamic},
         {oob_server, {oob_server, start_link, []},
            permanent, 10, worker, dynamic},
         {scgi_server, {scgi_server, start_link, [SCGIPort]},
            permanent, 10, worker, dynamic}
        ]
    }}.

stop(_State) ->
    ok.

