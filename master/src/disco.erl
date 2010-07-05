-module(disco).

-export([get_setting/1, has_setting/1, settings/0, oob_name/1]).

-spec oob_name(string()) -> string().
oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

-spec get_setting(string()) -> string().
get_setting(SettingName) ->
    case os:getenv(SettingName) of
        false ->
            error_logger:warning_report(
              {"Required setting", SettingName, "missing!"}),
            exit(["Must specify ", SettingName]);
        Val ->
            Val
    end.

-spec has_setting(string()) -> boolean().
has_setting(SettingName) ->
    case os:getenv(SettingName) of
        false -> false;
        ""    -> false;
        _Val  -> true
    end.

-spec settings() -> [string()].
settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

