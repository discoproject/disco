-module(disco_main).
-behaviour(supervisor).
-behaviour(application).

-compile([verbose, report_errors, report_warnings, trace, debug_info]).
-define(MAX_R, 10).
-define(MAX_T, 60).

-export([init/1, start/2, stop/1]).

conf(P) ->
        case application:get_env(P) of
                undefined -> exit(["Specify ", P]);
                {ok, Val} -> Val
        end.

write_pid(false) -> ok;
write_pid(PidFile) ->
        case file:write_file(PidFile, os:getpid()) of
                ok -> ok;
                Error -> exit(
                        ["Could not write PID to ", PidFile, ":", Error])
        end.

resultfs_enabled() ->
        case os:getenv("DISCO_FLAGS") of
                false -> false;
                Flags ->
                        lists:any(fun(X) ->
                                string:to_lower(X) == "resultfs" 
                        end, string:tokens(Flags, " "))
        end.


start(_Type, _Args) ->
        write_pid(os:getenv("DISCO_PID_FILE")),

        ScgiPort = conf(scgi_port),
        _MasterHostname = conf(disco_master_host),
        _DiscoConfig = conf(disco_config),
        _DiscoLocal = conf(disco_localdir),
        _DiscoRoot = conf(disco_root),
        _DiscoName = conf(disco_name),
        _DiscoSlavesOS = conf(disco_slaves_os),
        supervisor:start_link(disco_main, [ScgiPort]).

init([ScgiPort]) -> 
        error_logger:info_report([{"DISCO BOOTS"}]),
        
        ResultFS = resultfs_enabled(),
        {ok, App} = application:get_application(), 
        application:set_env(App, resultfs_enabled, ResultFS),
        error_logger:info_report({"Resultfs enabled:", App, ResultFS}),
        
        {ok, {{one_for_one, ?MAX_R, ?MAX_T},
                 [{event_server, {event_server, start_link, []},
                        permanent, 10, worker, dynamic},
                 {disco_server, {disco_server, start_link, []},
                        permanent, 10, worker, dynamic},
                 {job_queue, {job_queue, start_link, []},
                        permanent, 10, worker, dynamic},
                 {oob_server, {oob_server, start_link, []},
                        permanent, 10, worker, dynamic},
                 {scgi_server, {scgi_server, start_link, [ScgiPort]},
                        permanent, 10, worker, dynamic}
                ]
        }}.

stop(_State) ->
    ok.

