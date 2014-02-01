-module(worker_utils).

-include("common_types.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-export([annotate_input/1]).

-spec annotate_input(data_input()) -> {ok, data_input()} | {error, term()}.
annotate_input({data, _} = I) ->
    % Note: we could compute data_size() here if really needed.
    {ok, I};
annotate_input({dir, {Host, DirUrl, []}}) ->
    Url = disco:dir_to_url(DirUrl),
    Fetch = try
                {ok, {_S, _H, Content}} = httpc:request(get, {Url, []},
                    [{version, "HTTP/1.0"}], []),
                {ok, Content}
            catch K:V ->
                    lager:error("Error (~p:~p) fetching ~p (~p): ~p",
                                [K, V, Url, DirUrl, erlang:get_stacktrace()]),
                    {error, DirUrl}
            end,
    case Fetch of
        {ok, C} ->
            {ok, {dir, {Host, DirUrl, labelled_sizes(C)}}};
        {error, _} = Err ->
            Err
    end;
annotate_input({dir, {_H, _U, [_|_]}} = I) ->
    {ok, I}.

labelled_sizes(C) when is_list(C) ->
    labelled_sizes(list_to_binary(C));
labelled_sizes(C) ->
    % TODO: We need to handle dir files with the old format,
    % i.e. without sizes.
    {match, Lines} = re:run(C, "(.*?) (.*?) (.*?)\n",
                            [global, {capture, all_but_first, binary}]),
    LabelSizes = lists:sort([{list_to_integer(binary_to_list(L)),
                              list_to_integer(binary_to_list(S))}
                             || [L, _, S] <- Lines]),
    [labelled_group(Group) || Group <- disco_util:groupby(1, LabelSizes)].

labelled_group(Group) ->
    {[L|_], Sizes} = lists:unzip(Group),
    {L, lists:sum(Sizes)}.
