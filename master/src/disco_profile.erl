-module(disco_profile).
-export([timed_run/2, new_histogram/1, get_app_names/0, start_apps/0]).

-define(FOLSOM_HISTOGRAM_SIZE, 1000).

is_profile_on() ->
    case disco:get_setting("DISCO_PROFILE") of
        "False" -> false;
        "" -> false;
        "True" -> true
    end.

get_apps() ->
    [bear, meck, folsom, folsomite].

-spec get_app_names() -> [term()].
get_app_names() ->
    case is_profile_on() of
        true -> [io_lib:write_atom(Term) || Term <- get_apps()];
        false -> []
    end.

-spec start_apps() -> ok.
start_apps() ->
    [ok, ok, ok, ok] =
    case is_profile_on() of
        true ->
            [application:start(Name) || Name <- [bear, meck, folsom, folsomite]];
        false ->
            [ok, ok, ok, ok]
    end,
    ok.

-spec new_histogram(atom()) -> ok.
new_histogram(Name) ->
    case is_profile_on() of
        true ->
            lager:info("Adding histogram ~p", [Name]),
            folsom_metrics:new_histogram(Name, exdec, ?FOLSOM_HISTOGRAM_SIZE);
        false -> ok
    end.

-spec timed_run(fun(), atom()) -> term().
timed_run(Fun, Name) ->
    case is_profile_on() of
        true ->
            Start = os:timestamp(),
            Ret = Fun(),
            folsom_metrics:notify(Name, timer:now_diff(os:timestamp(), Start)),
            Ret;
        false ->
            Fun()
    end.

