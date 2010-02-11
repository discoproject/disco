-module(disco).

-export([get_setting/1, is_resultfs_enabled/0]).

get_setting(SettingName) ->
    case os:getenv(SettingName) of
        false  ->
            exit(["Must specify ", SettingName]);
        Val ->
            Val
    end.

is_resultfs_enabled() ->
    lists:any(fun(X) ->
                  string:to_lower(X) == "resultfs"
          end,
          string:tokens(disco:get_setting("DISCO_FLAGS"), " ")).
