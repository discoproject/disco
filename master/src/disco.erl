-module(disco).

-export([get_setting/1, has_setting/1,
         host/1, node/1, node_name/0, node_safe/1,
         oob_name/1,
         settings/0]).

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

host(Node) ->
    string:sub_word(atom_to_list(Node), 2, $@).

node(Host) ->
    list_to_atom(node_name() ++ "@" ++ Host).

node_name() ->
    get_setting("DISCO_NAME") ++ "_slave".

node_safe(Host) ->
    case catch list_to_existing_atom(node_name() ++ "@" ++ Host) of
        {'EXIT', _} -> false;
        Node -> Node
    end.

oob_name(JobName) ->
    lists:flatten(["disco:job:oob:", JobName]).

settings() ->
    lists:filter(fun has_setting/1,
                 string:tokens(get_setting("DISCO_SETTINGS"), ",")).

