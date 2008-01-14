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

start(_Type, _Args) ->
        ScgiPort = conf(scgi_port),
        _DiscoConfig = conf(disco_config),
        supervisor:start_link(disco_main, [ScgiPort]).

init([ScgiPort]) -> 
        error_logger:info_report([{'DISCO BOOTS'}]),

        {ok, {{one_for_one, ?MAX_R, ?MAX_T},
                 [{disco_server, {disco_server, start_link, []},
                        permanent, 10, worker, dynamic},
                 {scgi_server, {scgi_server, start_link, [ScgiPort]},
                        permanent, 10, worker, dynamic}
                ]
        }}.

stop(_State) ->
    ok.

