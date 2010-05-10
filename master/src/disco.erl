-module(disco).

-export([get_setting/1, has_setting/1, settings/0, oob_name/1]).

oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

get_setting(SettingName) ->
    case os:getenv(SettingName) of
        false ->
            error_logger:warning_report(
              {"Required setting", SettingName, "missing!"}),
            exit(["Must specify ", SettingName]);
        Val ->
            Val
    end.

has_setting(SettingName) ->
    case os:getenv(SettingName) of
        false -> false;
        ""    -> false;
        _Val  -> true
    end.

settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

