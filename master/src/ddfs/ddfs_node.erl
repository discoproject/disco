-module(ddfs_node).
-behaviour(gen_server).

-export([get_vols/0, gate_get_blob/0, put_blob/1, get_tag_data/3, rescan_tags/0]).

-export([start_link/2, init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

-include("gs_util.hrl").
-include("common_types.hrl").
-include("config.hrl").
-include("ddfs.hrl").
-include("ddfs_tag.hrl").

-record(state, {nodename :: string(),
                root :: path(),
                vols :: [volume()],
                putq :: http_queue:q(),
                getq :: http_queue:q(),
                tags :: gb_tree(),
                scanner :: pid()}).
-type state() :: #state{}.

-spec start_link(term(), pid()) -> no_return().
start_link(Config, NodeMon) ->
    process_flag(trap_exit, true),
    case gen_server:start_link(
                 {local, ?MODULE}, ?MODULE, Config, [{timeout, ?NODE_STARTUP}]) of
        {ok, _Server} ->
            error_logger:info_msg("~p starts on ~p", [?MODULE, node()]),
            ok;
        {error, {already_started, _Server}} ->
            error_logger:info_msg("~p already started on ~p", [?MODULE, node()]),
            exit(already_started);
        Error ->
            error_logger:info_msg("~p failed to start on ~p: ~p", [?MODULE, node(), Error]),
            exit(Error)
    end,
    NodeMon ! {node_ready, node()},
    receive
        {'EXIT', _, Reason0} ->
            error_logger:info_msg("~p exited on ~p: ~p", [?MODULE, node(), Reason0]),
            exit(Reason0)
    end.

-spec get_vols() -> {[volume()], path()}.
get_vols() ->
    gen_server:call(?MODULE, get_vols).

-spec gate_get_blob() -> ok | full.
gate_get_blob() ->
    gen_server:call(?MODULE, get_blob, ?GET_WAIT_TIMEOUT).

-type put_blob_result() :: {ok, path(), url()} | full
                         | {error, path(), term()} | {error, term()}.
-spec put_blob(nonempty_string()) -> put_blob_result().
put_blob(BlobName) ->
    gen_server:call(?MODULE, {put_blob, BlobName}, ?PUT_WAIT_TIMEOUT).

-spec rescan_tags() -> ok.
rescan_tags() ->
    gen_server:cast(?MODULE, rescan_tags).

-spec get_tag_data(node(), tagid(), taginfo()) -> {ok, binary()} | {error, term()}.
get_tag_data(SrcNode, TagId, TagNfo) ->
    Call = {get_tag_data, TagId, TagNfo},
    gen_server:call({?MODULE, SrcNode}, Call, ?NODE_TIMEOUT).

-spec init(proplists:proplist()) -> gs_init().
init(Config) ->
    {nodename, NodeName} = proplists:lookup(nodename, Config),
    {ddfs_root, DdfsRoot} = proplists:lookup(ddfs_root, Config),
    {disco_root, DiscoRoot} = proplists:lookup(disco_root, Config),
    {put_port, PutPort} = proplists:lookup(put_port, Config),
    {get_port, GetPort} = proplists:lookup(get_port, Config),
    {get_enabled, GetEnabled} = proplists:lookup(get_enabled, Config),
    {put_enabled, PutEnabled} = proplists:lookup(put_enabled, Config),

    {ok, Vols} = find_vols(DdfsRoot),
    {ok, Tags} = find_tags(DdfsRoot, Vols),

    if
        PutEnabled ->
            {ok, _PutPid} = ddfs_put:start(PutPort),
            ok;
        true ->
            ok
    end,
    if
        GetEnabled ->
            {ok, _GetPid} = ddfs_get:start(GetPort, {DdfsRoot, DiscoRoot}),
            ok;
        true ->
            ok
    end,

    Scanner = spawn_link(fun() -> refresh_tags(DdfsRoot, Vols) end),
    spawn_link(fun() -> monitor_diskspace(DdfsRoot, Vols) end),

    {ok, #state{nodename = NodeName,
                root = DdfsRoot,
                vols = Vols,
                tags = Tags,
                putq = http_queue:new(?HTTP_MAX_ACTIVE, ?HTTP_QUEUE_LENGTH),
                getq = http_queue:new(?HTTP_MAX_ACTIVE, ?HTTP_QUEUE_LENGTH),
                scanner = Scanner}}.

-type put_blob_msg() :: {put_blob, nonempty_string()}.
-type get_tag_ts_msg() :: {get_tag_timestamp, tagname()}.
-type get_tag_data_msg() :: {get_tag_data, tagid(),
                             {erlang:timestamp(), volume_name()}}.
-type put_tag_data_msg() :: {put_tag_data, {tagid(), binary()}}.
-type put_tag_commit_msg() :: {put_tag_commit, tagname(),
                               [{node(), volume_name()}]}.

-spec handle_call(get_tags, from(), state()) ->
                         gs_reply([tagname()]);
                 (get_vols, from(), state()) ->
                         gs_reply({[volume()], path()});
                 (get_blob, from(), state()) ->
                         gs_reply(full) | gs_noreply();
                 (get_diskspace, from(), state()) ->
                         gs_reply(diskinfo());
                 (put_blob_msg(), from(), state()) ->
                         gs_reply(put_blob_result()) | gs_noreply();
                 (get_tag_ts_msg(), from(), state()) ->
                         gs_reply(tag_ts());
                 (get_tag_data_msg(), from(), state()) ->
                         gs_noreply();
                 (put_tag_data_msg(), from(), state()) ->
                         gs_reply(put_tag_data_result());
                 (put_tag_commit_msg(), from(), state()) ->
                         gs_reply({ok, url()} | {error, _}).
handle_call(get_tags, _, #state{tags = Tags} = S) ->
    {reply, gb_trees:keys(Tags), S};

handle_call(get_vols, _, #state{vols = Vols, root = Root} = S) ->
    {reply, {Vols, Root}, S};

handle_call(get_blob, From, S) ->
    do_get_blob(From, S);

handle_call(get_diskspace, _From, S) ->
    {reply, do_get_diskspace(S), S};

handle_call({put_blob, BlobName}, From, S) ->
    do_put_blob(BlobName, From, S);

handle_call({get_tag_timestamp, TagName}, _From, S) ->
    {reply, do_get_tag_timestamp(TagName, S), S};

handle_call({get_tag_data, TagId, {_Time, VolName}}, From, State) ->
    spawn(fun() -> do_get_tag_data(TagId, VolName, From, State) end),
    {noreply, State};

handle_call({put_tag_data, {Tag, Data}}, _From, S) ->
    {reply, do_put_tag_data(Tag, Data, S), S};

handle_call({put_tag_commit, Tag, TagVol}, _, S) ->
    {Reply, S1} = do_put_tag_commit(Tag, TagVol, S),
    {reply, Reply, S1}.

-type casts() :: rescan_tags
               | {update_vols, [volume()]}
               | {update_tags, gb_tree()}.
-spec handle_cast(casts(), state()) -> gs_noreply().
handle_cast(rescan_tags, #state{scanner = Scanner} = S) ->
    Scanner ! rescan,
    {noreply, S};

handle_cast({update_vols, NewVols}, #state{vols = Vols} = S) ->
    {noreply, S#state{vols = lists:ukeymerge(2, NewVols, Vols)}};

handle_cast({update_tags, Tags}, S) ->
    {noreply, S#state{tags = Tags}}.

-spec handle_info({'DOWN', _, _, pid(), _}, state()) -> gs_noreply().
handle_info({'DOWN', _, _, Pid, _}, #state{putq = PutQ, getq = GetQ} = S) ->
    % We don't know if Pid refers to a put or get request.
    % We can safely try to remove it from both the queues: it can exist in
    % one of the queues at most.
    {_, NewPutQ} = http_queue:remove(Pid, PutQ),
    {_, NewGetQ} = http_queue:remove(Pid, GetQ),
    {noreply, S#state{putq = NewPutQ, getq = NewGetQ}}.

% callback stubs
-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ===================================================================
%% internal functions

-spec do_get_blob({pid(), _}, state()) -> gs_reply(full) | gs_noreply().
do_get_blob({Pid, _Ref} = From, #state{getq = Q} = S) ->
    Reply = fun() -> gen_server:reply(From, ok) end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{getq = NewQ}}
    end.

-spec do_get_diskspace(state()) -> diskinfo().
do_get_diskspace(#state{vols = Vols}) ->
    lists:foldl(fun ({{Free, Used}, _VolName}, {TotalFree, TotalUsed}) ->
                        {TotalFree + Free, TotalUsed + Used}
                end, {0, 0}, Vols).

-spec do_put_blob(nonempty_string(), {pid(), _}, state())
                 -> gs_reply(put_blob_result()) | gs_noreply().
do_put_blob(_BlobName, _From, #state{vols = []} = S) ->
    {reply, {error, no_volumes}, S};
do_put_blob(BlobName, {Pid, _Ref} = From,
            #state{putq = Q, nodename = NodeName,
                   root = Root, vols = Vols} = S) ->
    Reply = fun() ->
                    {_Space, VolName} = choose_vol(Vols),
                    {ok, Local, Url} = ddfs_util:hashdir(list_to_binary(BlobName),
                                                         NodeName, "blob",
                                                         Root, VolName),
                    case ddfs_util:ensure_dir(Local) of
                        ok ->
                            gen_server:reply(From, {ok, Local, Url});
                        {error, E} ->
                            gen_server:reply(From, {error, Local, E})
                    end
            end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{putq = NewQ}}
    end.

-type tag_ts() :: not_found | {ok, {erlang:timestamp(), volume_name()}}.
-spec do_get_tag_timestamp(tagname(), state()) -> tag_ts().
do_get_tag_timestamp(TagName, #state{tags = Tags}) ->
    case gb_trees:lookup(TagName, Tags) of
        none ->
            notfound;
        {value, {_Time, _VolName} = TagNfo} ->
            {ok, TagNfo}
    end.

-spec do_get_tag_data(tagid(), volume_name(), {pid(), _}, state()) -> ok.
do_get_tag_data(TagId, VolName, From, #state{root = Root}) ->
    {ok, TagDir, _Url} = ddfs_util:hashdir(TagId,
                                           disco:host(node()),
                                           "tag",
                                           Root,
                                           VolName),
    TagPath = filename:join(TagDir, binary_to_list(TagId)),
    case prim_file:read_file(TagPath) of
        {ok, Binary} ->
            gen_server:reply(From, {ok, Binary});
        {error, Reason} ->
            error_logger:warning_msg("Read failed at ~p: ~p", [TagPath, Reason]),
            gen_server:reply(From, {error, read_failed})
    end.

-type put_tag_data_result() :: {ok, volume_name()} | {error, _}.
-spec do_put_tag_data(tagname(), binary(), state()) -> put_tag_data_result().
do_put_tag_data(_Tag, _Data, #state{vols = []}) ->
    {error, no_volumes};
do_put_tag_data(Tag, Data, #state{nodename = NodeName,
                                  vols = Vols,
                                  root = Root}) ->
    {_Space, VolName} = choose_vol(Vols),
    {ok, Local, _} = ddfs_util:hashdir(Tag,
                                       NodeName,
                                       "tag",
                                       Root,
                                       VolName),
    case ddfs_util:ensure_dir(Local) of
        ok ->
            Partial = lists:flatten(["!partial.", binary_to_list(Tag)]),
            Filename = filename:join(Local, Partial),
            case prim_file:write_file(Filename, Data) of
                ok ->
                    {ok, VolName};
                {error, _} = E ->
                    E
            end;
        E ->
            E
    end.

-spec do_put_tag_commit(tagname(), [{node(), volume_name()}], state())
                       -> {{ok, url()} | {error, _}, state()}.
do_put_tag_commit(Tag, TagVol, #state{nodename = NodeName,
                                      root = Root,
                                      tags = Tags} = S) ->
    {_, VolName} = lists:keyfind(node(), 1, TagVol),
    {ok, Local, Url} = ddfs_util:hashdir(Tag,
                                         NodeName,
                                         "tag",
                                         Root,
                                         VolName),
    {TagName, Time} = ddfs_util:unpack_objname(Tag),

    TagL = binary_to_list(Tag),
    Src = filename:join(Local, lists:flatten(["!partial.", TagL])),
    Dst = filename:join(Local,  TagL),
    case ddfs_util:safe_rename(Src, Dst) of
        ok ->
            {{ok, Url},
             S#state{tags = gb_trees:enter(TagName, {Time, VolName}, Tags)}};
        {error, _} = E ->
            {E, S}
    end.


-spec try_makedir(path()) -> ok | error.
try_makedir(Dir) ->
    case prim_file:make_dir(Dir) of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        Error ->
            error_logger:warning_msg(
              "Error initializing directory ~p: ~p. This volume will be ignored!",
              [Dir, Error]),
            error
    end.

-spec init_vols(path(), [volume_name()]) -> {ok, [volume()]}.
init_vols(Root, VolNames) ->
    _ = [begin
             ok = try_makedir(filename:join([Root, VolName, "blob"])),
             ok = try_makedir(filename:join([Root, VolName, "tag"]))
         end || VolName <- VolNames],
    {ok, [{{0, 0}, VolName} || VolName <- lists:sort(VolNames)]}.

-spec find_vols(path()) -> eof | ok | {ok, [volume()]} | {error, _}.
find_vols(Root) ->
    case prim_file:list_dir(Root) of
        {ok, Files} ->
            case [F || "vol" ++ _ = F <- Files] of
                [] ->
                    VolName = "vol0",
                    ok = try_makedir(filename:join([Root, VolName])),
                    init_vols(Root, [VolName]);
                VolNames ->
                    init_vols(Root, VolNames)
            end;
        {error, enoent} ->
            error_logger:info_msg("Creating new root directory ~p", [Root]),
            ok = try_makedir(Root),
            find_vols(Root);
        Error ->
            error_logger:warning_msg("Invalid root directory ~p: ~p", [Root, Error]),
            Error
    end.

-spec find_tags(path(), [volume()]) -> {ok, gb_tree()}.
find_tags(Root, Vols) ->
    {ok,
     lists:foldl(fun({_Space, VolName}, Tags) ->
                         ddfs_util:fold_files(filename:join([Root, VolName, "tag"]),
                                              fun(Tag, _, Tags1) ->
                                                      parse_tag(Tag, VolName, Tags1)
                                              end, Tags)
                 end, gb_trees:empty(), Vols)}.

-spec parse_tag(nonempty_string(), volume_name(), gb_tree()) -> gb_tree().
parse_tag("!" ++ _, _, Tags) -> Tags;
parse_tag(Tag, VolName, Tags) ->
    {TagName, Time} = ddfs_util:unpack_objname(Tag),
    case gb_trees:lookup(TagName, Tags) of
        none ->
            gb_trees:insert(TagName, {Time, VolName}, Tags);
        {value, {OTime, _}} when OTime < Time ->
            gb_trees:enter(TagName, {Time, VolName}, Tags);
        _ ->
            Tags
    end.

-spec choose_vol([volume()]) -> volume().
choose_vol(Vols) ->
    % Choose the volume with most available space.  Note that the key
    % being sorted is the diskinfo() tuple, which has free-space as
    % the first element.
    [Vol|_] = lists:reverse(lists:keysort(1, Vols)),
    Vol.

-spec monitor_diskspace(path(), [volume()]) -> no_return().
monitor_diskspace(Root, Vols) ->
    timer:sleep(?DISKSPACE_INTERVAL),
    Fold = fun({_OldSpace, VolName}, VolAcc) ->
                   case ddfs_util:diskspace(filename:join([Root, VolName])) of
                       {ok, Space} -> [{Space, VolName} | VolAcc];
                       _ -> VolAcc
                   end
           end,
    NewVols = lists:reverse(lists:foldl(Fold, [], Vols)),
    gen_server:cast(ddfs_node, {update_vols, NewVols}),
    monitor_diskspace(Root, NewVols).

-spec refresh_tags(path(), [volume()]) -> no_return().
refresh_tags(Root, Vols) ->
    receive
        rescan ->
            ok
    after ?FIND_TAGS_INTERVAL ->
            ok
    end,
    {ok, Tags} = find_tags(Root, Vols),
    gen_server:cast(ddfs_node, {update_tags, Tags}),
    refresh_tags(Root, Vols).
