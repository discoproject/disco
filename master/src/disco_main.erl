-module(disco_main).

-export([init/1, start/2, stop/1]).

-behaviour(supervisor).
-behaviour(application).

-include("common_types.hrl").
-include("config.hrl").

-define(MAX_R, 10).
-define(MAX_T, 60).

set_env(Key, Default) ->
    Val =
        case application:get_env(Key) of
            undefined ->
                Default;
            {ok, X} ->
                X
        end,
    ok = application:set_env(disco, Key, Val).

init_settings() ->
    set_env(max_failure_rate, ?TASK_MAX_FAILURES).

-spec write_pid(path()) -> ok.
write_pid(PidFile) ->
    case file:write_file(PidFile, os:getpid()) of
        ok -> ok;
        Error ->
            exit(["Could not write PID to ", PidFile, ":", Error])
    end.

-spec start(_, _) -> {ok, pid()} | {error, term()}.
start(_Type, _Args) ->
    ok = application:start(lager),
    init_settings(),
    write_pid(disco:get_setting("DISCO_MASTER_PID")),
    Port = disco:get_setting("DISCO_PORT"),
    supervisor:start_link(disco_main, [list_to_integer(Port)]).

-spec init([non_neg_integer()]) ->
        {ok, {{one_for_one, ?MAX_R, ?MAX_T}, [supervisor:child_spec()]}}.
init([Port]) ->
    lager:info("DISCO BOOTS"),
    {ok, {{one_for_one, ?MAX_R, ?MAX_T}, [
         {disco_proxy, {disco_proxy, start, []},
            permanent, 10, worker, dynamic},
         {ddfs_master, {ddfs_master, start_link, []},
            permanent, 10, worker, dynamic},
         {event_server, {event_server, start_link, []},
            permanent, 10, worker, dynamic},
         {disco_config, {disco_config, start_link, []},
            permanent, 10, worker, dynamic},
         {disco_server, {disco_server, start_link, []},
            permanent, 10, worker, dynamic},
         {mochi_server, {web_server, start, [Port]},
            permanent, 10, worker, dynamic}
        ]
    }}.

-spec stop(_) -> ok.
stop(_State) ->
    ok.

