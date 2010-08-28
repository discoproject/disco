-module(shuffle).
-export([combine_tasks/3, combine_tasks_node/4, process_url/3]).

combine_tasks(Name, Mode, DirUrls) ->
    DataRoot = disco:get_setting("DISCO_DATA"),
    JobHome = disco:jobhome(Name),
    NodeGroups = disco_util:groupby(1, lists:keysort(1, DirUrls)),
    Messages = [begin
                   {[Node|_], NodeDirUrls} = lists:unzip(NodeUrls),
                   {Node, [DataRoot, JobHome, Mode, NodeDirUrls]}
                end || NodeUrls <- NodeGroups],
    {ok, wait_replies(Name, call_nodes(Name, Messages, 0), [])}.

call_nodes(Name, Messages, FailCount) ->
    {ok, MaxFail} = application:get_env(max_failure_rate),
    if FailCount >= MaxFail ->
        event_server:event(Name,
            "ERROR: Shuffling failed ~B times. At most ~B failures "
            "are allowed. Aborting job.",
            [FailCount, MaxFail], []),
        throw(logged_error);
    true ->
        call_nodes_do(Messages, FailCount + 1)
    end.

call_nodes_do(Messages, FailCount) ->
    Promises =
        [begin
            Key = rpc:async_call(Node, shuffle, combine_tasks_node, Args),
            {Key, Msg}
         end || {Node, Args} = Msg <- Messages],
    {Promises, FailCount}.

wait_replies(_Name, {[], _FailCount}, Results) -> Results;
wait_replies(Name, {Promises, FailCount}, Results) ->
    Replies = [{rpc:yield(Key), Msg} || {Key, Msg} <- Promises],
    Fun = fun({{ok, _}, _}) -> true; (_) -> false end,
    {OkReplies, Failed} = lists:partition(Fun, Replies),
    Messages =
        [begin
            M = "WARN: Creating index file failed at ~s: ~p (retrying)",
            event_server:event(Name, M, [disco:host(Node), Reply], {}),
            timer:sleep(10000),
            Msg
         end || {Reply, {Node, _Arg} = Msg} <- Failed],
    Ok = Results ++ [Url || {{ok, Url}, _} <- OkReplies],
    wait_replies(Name, call_nodes(Name, Messages, FailCount), Ok).

% NB: Under rare circumstances it is possible that several instances of
% combine_tasks_node are running in parallel on a single node. Thus, all
% operations it performs on shared resources should be atomic.

combine_tasks_node(DataRoot, JobHome, Mode, DirUrls) ->
    process_flag(priority, low),
    Host = disco:host(node()),
    JobRoot = filename:join([DataRoot, Host, JobHome]),

    PartDir = ["partitions-", ddfs_util:timestamp()],
    PartPath = filename:join(JobRoot, PartDir),
    PartUrl = ["disco://", Host, "/disco/", Host, "/", JobHome, PartDir],

    IndexFile = [Mode, "-index.txt.gz"],
    IndexPath = filename:join([JobRoot, IndexFile]),
    IndexUrl = ["dir://", Host, "/disco/", Host, "/", JobHome, IndexFile],

    prim_file:make_dir(PartPath),
    Index = merged_index(DirUrls, DataRoot, {PartPath, PartUrl}),
    ok = write_index(IndexPath, Index),
    {ok, list_to_binary(IndexUrl)}.

write_index(Path, Lines) ->
    Time = ddfs_util:timestamp(),
    {ok, IO} = prim_file:open([Path, $., Time], [write, raw, compressed]),
    ok = prim_file:write(IO, Lines),
    ok = prim_file:close(IO),
    prim_file:rename([Path, $., Time], Path).

merged_index(DirUrls, DataRoot, PartInfo) ->
    gb_sets:to_list(
        lists:foldl(
            fun(Url, UrlSet) ->
                Set = gb_sets:from_list(process_task(Url, DataRoot, PartInfo)),
                gb_sets:union(UrlSet, Set)
            end, gb_sets:empty(), DirUrls)).

process_task(DirUrl, DataRoot, PartInfo) ->
    TaskPath = disco:disco_url_path(DirUrl),
    {ok, Index} = prim_file:read_file(filename:join(DataRoot, TaskPath)),
    Lines = parse_index(Index),
    [process_url(E, DataRoot, PartInfo) || E <- Lines].

% NB: process_url is called for each output file produced on a node,
% i.e. potentially millions of times for a job. Be careful when making
% any changes to it. Especially measure the performance impact of your
% changes!
process_url([Id, <<"part://", _/binary>> = Url],
            DataRoot,
            {PartPath, PartUrl}) ->
    PartFile = ["part-", Id],
    PartSrc = [DataRoot, "/", disco:disco_url_path(Url)],
    PartDst = [PartPath, "/", PartFile],
    ok = ddfs_util:concatenate(PartSrc, PartDst),
    list_to_binary([Id, " ", PartUrl, "/", PartFile, "\n"]);

process_url([Id, Url], _DataRoot, _PartInfo) ->
    list_to_binary([Id, " ", Url, "\n"]).

parse_index(Index) ->
    {match, Lines} = re:run(Index,
                            "(.*?) (.*?)\n",
                            [global, {capture, all_but_first, binary}]),
    Lines.

