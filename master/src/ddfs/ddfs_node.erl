-module(ddfs_node).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-include("config.hrl").

% Diskinfo is {FreeSpace, UsedSpace}.
-type diskinfo() :: {non_neg_integer(), non_neg_integer()}.
-record(state, {nodename :: string(),
                root :: nonempty_string(),
                vols :: [{diskinfo(), nonempty_string()},...],
                putq :: http_queue:q(),
                getq :: http_queue:q(),
                tags :: gb_tree()}).

start_link(Config) ->
    process_flag(trap_exit, true),
    error_logger:info_report([{"DDFS node starts"}]),
    case catch gen_server:start_link(
            {local, ddfs_node}, ddfs_node, Config, [{timeout, ?NODE_STARTUP}]) of
        {ok, _Server} ->
            ok;
        {error, {already_started, _Server}} ->
            exit(already_started);
        {'EXIT', Reason} ->
            exit(Reason)
    end,
    receive
        {'EXIT', _, Reason0} ->
            exit(Reason0)
    end.

stop() ->
    gen_server:call(ddfs_node, stop).

init(Config) ->
    {nodename, NodeName} = proplists:lookup(nodename, Config),
    {ddfs_root, DdfsRoot} = proplists:lookup(ddfs_root, Config),
    {disco_root, DiscoRoot} = proplists:lookup(disco_root, Config),
    {put_max, PutMax} = proplists:lookup(put_max, Config),
    {get_max, GetMax} = proplists:lookup(get_max, Config),
    {put_port, PutPort} = proplists:lookup(put_port, Config),
    {get_port, GetPort} = proplists:lookup(get_port, Config),
    {get_enabled, GetEnabled} = proplists:lookup(get_enabled, Config),
    {put_enabled, PutEnabled} = proplists:lookup(put_enabled, Config),

    {ok, Vols} = find_vols(DdfsRoot),
    {ok, Tags} = find_tags(DdfsRoot, Vols),

    if
        PutEnabled ->
            {ok, _PutPid} = ddfs_put:start([{port, PutPort}]);
        true ->
            ok
    end,
    if
        GetEnabled ->
            {ok, _GetPid} = ddfs_get:start([{port, GetPort}],
                                           {DdfsRoot, DiscoRoot});
        true ->
            ok
    end,

    spawn_link(fun() -> refresh_tags(DdfsRoot, Vols) end),
    spawn_link(fun() -> monitor_diskspace(DdfsRoot, Vols) end),

    {ok, #state{nodename = NodeName,
                root = DdfsRoot,
                vols = Vols,
                tags = Tags,
                putq = http_queue:new(PutMax, ?HTTP_QUEUE_LENGTH),
                getq = http_queue:new(GetMax, ?HTTP_QUEUE_LENGTH)}}.

handle_call(get_tags, _, #state{tags = Tags} = S) ->
    {reply, gb_trees:keys(Tags), S};

handle_call(get_vols, _, #state{vols = Vols, root = Root} = S) ->
    {reply, {Vols, Root}, S};

handle_call(get_blob, {Pid, _Ref} = From, #state{getq = Q} = S) ->
    Reply = fun() -> gen_server:reply(From, ok) end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{getq = NewQ}}
    end;

handle_call(get_diskspace, _From, #state{vols = Vols} = S) ->
    {reply, lists:foldl(fun ({{Free, Used}, _VolName}, {TotalFree, TotalUsed}) ->
                                {TotalFree + Free, TotalUsed + Used}
                        end, {0, 0}, Vols), S};

handle_call({put_blob, BlobName}, {Pid, _Ref} = From, #state{putq = Q} = S) ->
    Reply = fun() ->
                    {_Space, VolName} = choose_vol(S#state.vols),
                    {ok, Local, Url} = ddfs_util:hashdir(list_to_binary(BlobName),
                                                         S#state.nodename,
                                                         "blob",
                                                         S#state.root,
                                                         VolName),
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
    end;

handle_call({get_tag_timestamp, TagName}, _From, S) ->
    case gb_trees:lookup(TagName, S#state.tags) of
        none ->
            {reply, notfound, S};
        {value, {_Time, _VolName} = TagNfo} ->
            {reply, {ok, TagNfo}, S}
    end;

handle_call({get_tag_data, TagName, TagNfo}, From, S) ->
    spawn(fun() ->
        read_tag(TagName, S#state.nodename, S#state.root, TagNfo, From)
    end),
    {noreply, S};

handle_call({put_tag_data, {Tag, Data}}, _From, S) ->
    {_Space, VolName} = choose_vol(S#state.vols),
    {ok, Local, _} = ddfs_util:hashdir(Tag,
                                       S#state.nodename,
                                       "tag",
                                       S#state.root,
                                       VolName),
    case ddfs_util:ensure_dir(Local) of
        ok ->
            Filename = filename:join(Local, ["!partial.", binary_to_list(Tag)]),
            {reply, case prim_file:write_file(Filename, Data) of
                        ok ->
                            {ok, VolName};
                        {error, _} = E ->
                            E
                    end, S};
        E ->
            {reply, E, S}
    end;

handle_call({put_tag_commit, Tag, TagVol}, _, S) ->
    {value, {_, VolName}} = lists:keysearch(node(), 1, TagVol),
    {ok, Local, Url} = ddfs_util:hashdir(Tag,
                                         S#state.nodename,
                                         "tag",
                                         S#state.root,
                                         VolName),
    {TagName, Time} = ddfs_util:unpack_objname(Tag),

    TagL = binary_to_list(Tag),
    Src = filename:join(Local, ["!partial.", TagL]),
    Dst = filename:join(Local,  TagL),
    case ddfs_util:safe_rename(Src, Dst) of
        ok ->
            {reply,
             {ok, Url},
             S#state{tags = gb_trees:enter(TagName,
                                           {Time, VolName},
                                           S#state.tags)}};
        {error, _} = E ->
            {reply, E, S}
    end.

handle_cast({update_vols, NewVols}, #state{vols = Vols} = S) ->
    {noreply, S#state{vols = lists:ukeymerge(2, NewVols, Vols)}};

handle_cast({update_tags, Tags}, S) ->
    {noreply, S#state{tags = Tags}}.

handle_info({'DOWN', _, _, Pid, _}, #state{putq = PutQ, getq = GetQ} = S) ->
    % We don't know if Pid refers to a put or get request.
    % We can safely try to remove it both the queues: It can exist in
    % one of the queues at most.
    {_, NewPutQ} = http_queue:remove(Pid, PutQ),
    {_, NewGetQ} = http_queue:remove(Pid, GetQ),
    {noreply, S#state{putq = NewPutQ, getq = NewGetQ}}.

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

-spec read_tag(binary(), nonempty_string(), nonempty_string(),
               {_, nonempty_string()}, {pid(), reference()}) -> _.
read_tag(Tag, NodeName, Root, {_, VolName}, From) ->
    {ok, D, _} = ddfs_util:hashdir(Tag, NodeName, "tag", Root, VolName),
    case prim_file:read_file(filename:join(D, binary_to_list(Tag))) of
        {ok, Bin} ->
            gen_server:reply(From, {ok, Bin});
        E ->
            error_logger:warning_report({"Read failed", Tag, D, E}),
            gen_server:reply(From, {error, read_failed})
    end.

-spec init_vols(nonempty_string(), [nonempty_string(),...]) ->
    {'ok', [{diskinfo(), nonempty_string()},...]}.
init_vols(Root, VolNames) ->
    lists:foreach(fun(VolName) ->
                          prim_file:make_dir(filename:join([Root, VolName, "blob"])),
                          prim_file:make_dir(filename:join([Root, VolName, "tag"]))
                  end, VolNames),
    {ok, [{{0, 0}, VolName} || VolName <- lists:sort(VolNames)]}.

-spec find_vols(nonempty_string()) ->
    {'ok', [{diskinfo(), nonempty_string()}]} | {'error', _}.
find_vols(Root) ->
    case prim_file:list_dir(Root) of
        {ok, Files} ->
            case [F || "vol" ++ _ = F <- Files] of
                [] ->
                    VolName = "vol0",
                    prim_file:make_dir(filename:join([Root, VolName])),
                    error_logger:warning_report({"Could not find volumes in ", Root,
                                                 "Created ", VolName}),
                    init_vols(Root, [VolName]);
                VolNames ->
                    init_vols(Root, VolNames)
            end;
        Error ->
            error_logger:warning_report(
                {"Invalid root directory", Root, Error}),
            Error
    end.

-spec find_tags(nonempty_string(), [{diskinfo(), nonempty_string()},...]) ->
    {'ok', gb_tree()}.
find_tags(Root, Vols) ->
    {ok,
     lists:foldl(fun({_Space, VolName}, Tags) ->
                         ddfs_util:fold_files(filename:join([Root, VolName, "tag"]),
                                              fun(Tag, _, Tags1) ->
                                                      parse_tag(Tag, VolName, Tags1)
                                              end, Tags)
                 end, gb_trees:empty(), Vols)}.

-spec parse_tag(nonempty_string(), nonempty_string(), gb_tree()) -> gb_tree().
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

-spec choose_vol([{diskinfo(), nonempty_string()},...]) ->
    {diskinfo(), nonempty_string()}.
choose_vol(Vols) ->
    % Choose the volume with most available space.  Note that the key
    % being sorted is the diskinfo() tuple, which has free-space as
    % the first element.
    [Vol|_] = lists:reverse(lists:keysort(1, Vols)),
    Vol.

-spec monitor_diskspace(nonempty_string(),
                        [{diskinfo(), nonempty_string()},...]) ->
    no_return().
monitor_diskspace(Root, Vols) ->
    timer:sleep(?DISKSPACE_INTERVAL),
    Df = fun(VolName) ->
            ddfs_util:diskspace(filename:join([Root, VolName]))
         end,
    NewVols = [{Space, VolName}
               || {VolName, {ok, Space}}
               <- [{VolName, Df(VolName)} || {_OldSpace, VolName} <- Vols]],
    gen_server:cast(ddfs_node, {update_vols, NewVols}),
    monitor_diskspace(Root, NewVols).

-spec refresh_tags(nonempty_string(), [{diskinfo(), nonempty_string()},...]) ->
    no_return().
refresh_tags(Root, Vols) ->
    timer:sleep(?FIND_TAGS_INTERVAL),
    {ok, Tags} = find_tags(Root, Vols),
    gen_server:cast(ddfs_node, {update_tags, Tags}),
    refresh_tags(Root, Vols).
