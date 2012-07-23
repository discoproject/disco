-module(shuffle).
-export([combine_tasks/3, combine_tasks_node/4, process_url/3]).

-include("common_types.hrl").
-include("disco.hrl").

-type msg() :: {node(), [term()]}.
-type promise() :: {pid(), msg()}.

-spec combine_tasks(jobname(), task_mode(), [msg()]) -> {ok, [binary()]}.
combine_tasks(JobName, Mode, DirUrls) ->
    DataRoot = disco:get_setting("DISCO_DATA"),
    NodeGroups = disco_util:groupby(1, lists:keysort(1, DirUrls)),
    Messages = [begin
                   {[Node|_], NodeDirUrls} = lists:unzip(NodeUrls),
                   {Node, [DataRoot, JobName, Mode, NodeDirUrls]}
                end || NodeUrls <- NodeGroups],
    {ok, wait_replies(JobName, call_nodes(Messages, 0), [])}.

-spec call_nodes([msg()], non_neg_integer()) ->
                        {[promise()], non_neg_integer()}.
call_nodes(Messages, FailCount) ->
    {ok, MaxFail} = application:get_env(max_failure_rate),
    if FailCount >= MaxFail ->
            throw({error,
                   {"Shuffling failed ~B times. At most ~B failures are allowed.",
                    [FailCount, MaxFail]}});
       true ->
            call_nodes_do(Messages, FailCount + 1)
    end.

-spec call_nodes_do([{node(), [term()]}], non_neg_integer()) ->
                           {[promise()], non_neg_integer()}.
call_nodes_do(Messages, FailCount) ->
    Promises =
        [begin
            Key = rpc:async_call(Node, shuffle, combine_tasks_node, Args),
            {Key, Msg}
         end || {Node, Args} = Msg <- Messages],
    {Promises, FailCount}.

-spec wait_replies(jobname(), {[promise()], non_neg_integer()}, [binary()])
                  -> [binary()].
wait_replies(_Name, {[], _FailCount}, Results) -> Results;
wait_replies(Name, {Promises, FailCount}, Results) ->
    Replies = [{rpc:yield(Key), Msg} || {Key, Msg} <- Promises],
    Fun = fun({{ok, _}, _}) -> true; (_) -> false end,
    {OkReplies, Failed} = lists:partition(Fun, Replies),
    Messages =
        [begin
            M = "WARNING: Creating index file failed at ~s: ~p (retrying)",
            event_server:event(Name, M, [disco:host(Node), Reply], none),
            timer:sleep(10000),
            Msg
         end || {Reply, {Node, _Arg} = Msg} <- Failed],
    Ok = Results ++ [Url || {{ok, Url}, _} <- OkReplies],
    wait_replies(Name, call_nodes(Messages, FailCount), Ok).

-type partinfo() :: {file:filename(), [nonempty_string()]}.
% NB: Under rare circumstances it is possible that several instances of
% combine_tasks_node are running in parallel on a single node. Thus, all
% operations it performs on shared resources should be atomic.

-spec combine_tasks_node(path(), jobname(), task_mode(),
                         [nonempty_string()]) -> {ok, binary()}.
combine_tasks_node(DataRoot, JobName, Mode, DirUrls) ->
    Host = disco:host(node()),
    JobHome = disco:jobhome(JobName, filename:join([DataRoot, Host])),
    ResultsHome = filename:join(JobHome, ".disco"),
    JobLink = disco:jobhome(JobName, filename:join([Host, "disco", Host])),
    ResultsLink = filename:join(JobLink, ".disco"),

    PartDir = lists:flatten(["partitions-", ddfs_util:timestamp()]),
    PartPath = filename:join(ResultsHome, PartDir),
    PartUrl = ["disco://", ResultsLink, "/", PartDir],

    IndexFile = lists:flatten([atom_to_list(Mode), "-index.txt.gz"]),
    IndexPath = filename:join(ResultsHome, IndexFile),
    IndexUrl = ["dir://", ResultsLink, "/", IndexFile],

    ok = prim_file:make_dir(PartPath),
    Index = merged_index(DirUrls, DataRoot, {PartPath, PartUrl}),
    ok = write_index(IndexPath, Index),
    {ok, list_to_binary(IndexUrl)}.

-spec write_index(file:filename(), [binary()]) -> ok.
write_index(Path, Lines) ->
    Time = ddfs_util:timestamp(),
    {ok, IO} = prim_file:open([Path, $., Time], [write, raw, compressed]),
    ok = prim_file:write(IO, Lines),
    ok = prim_file:close(IO),
    prim_file:rename([Path, $., Time], Path).

-spec merged_index([nonempty_string()], path(), partinfo()) -> [binary()].
merged_index(DirUrls, DataRoot, PartInfo) ->
    gb_sets:to_list(
        lists:foldl(
            fun(Url, UrlSet) ->
                Set = gb_sets:from_list(process_task(Url, DataRoot, PartInfo)),
                gb_sets:union(UrlSet, Set)
            end, gb_sets:empty(), DirUrls)).

-spec process_task(nonempty_string(), path(), partinfo()) -> [binary()].
process_task(DirUrl, DataRoot, PartInfo) ->
    TaskPath = disco:disco_url_path(DirUrl),
    {ok, Index} = prim_file:read_file(filename:join(DataRoot, TaskPath)),
    Lines = parse_index(Index),
    [process_url(E, DataRoot, PartInfo) || E <- Lines].

% NB: process_url is called for each output file produced on a node,
% i.e. potentially millions of times for a job. Be careful when making
% any changes to it. Especially measure the performance impact of your
% changes!
-spec process_url(_, path(), partinfo()) -> binary().
process_url([Id, <<"part://", _/binary>> = Url],
            DataRoot,
            {PartPath, PartUrl}) ->
    PartFile = ["part-", binary_to_list(Id)],
    PartSrc = [DataRoot, "/", disco:disco_url_path(Url)],
    PartDst = [PartPath, "/", PartFile],
    ok = ddfs_util:concatenate(PartSrc, PartDst),
    list_to_binary([Id, " ", PartUrl, "/", PartFile, "\n"]);

process_url([Id, Url], _DataRoot, _PartInfo) ->
    list_to_binary([Id, " ", Url, "\n"]).

-spec parse_index(binary()) -> _.
parse_index(<<"">>) ->
    [];
parse_index(Index) ->
    {match, Lines} = re:run(Index,
                            "(.*?) (.*?)\n",
                            [global, {capture, all_but_first, binary}]),
    Lines.
