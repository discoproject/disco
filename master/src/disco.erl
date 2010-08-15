-module(disco).

-export([get_setting/1, has_setting/1,
         host/1, node/1, node_name/0, node_safe/1,
         oob_name/1,
         settings/0]).

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

-spec has_setting(string()) -> bool().
has_setting(SettingName) ->
    case os:getenv(SettingName) of
        false -> false;
        ""    -> false;
        _Val  -> true
    end.

-spec host(node()) -> string().
host(Node) ->
    string:sub_word(atom_to_list(Node), 2, $@).

-spec node(string()) -> node().
node(Host) ->
    list_to_atom(node_name() ++ "@" ++ Host).

-spec node_name() -> string().
node_name() ->
    get_setting("DISCO_NAME") ++ "_slave".

-spec node_safe(string()) -> 'false' | node().
node_safe(Host) ->
    case catch list_to_existing_atom(node_name() ++ "@" ++ Host) of
        {'EXIT', _} -> false;
        Node -> Node
    end.

-spec oob_name(string()) -> string().
oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

-spec settings() -> [string()].
settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

