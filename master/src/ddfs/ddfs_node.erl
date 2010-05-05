-module(ddfs_node).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-include("config.hrl").

-record(state, {nodename, root, volumes, putq, getq, tags}).

start_link(Config) ->
    process_flag(trap_exit, true),
    error_logger:info_report([{"DDFS node starts"}]),
    case catch gen_server:start_link(
            {local, ddfs_node}, ddfs_node, Config, [{timeout, ?NODE_STARTUP}]) of
        {ok, _Server} -> ok;
        {error, {already_started, _Server}} -> ok;
        {'EXIT', Reason} -> exit(Reason)
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

    {ok, VolUn} = find_volumes(DdfsRoot),
    % lists:ukeymerge in update_volumestats requires a sorted list
    Vol = lists:sort(VolUn),
    {ok, Tags} = find_tags(DdfsRoot, Vol),
    if PutEnabled ->
        {ok, _PutPid} =
            ddfs_put:start([{port, PutPort}]);
    true ->
        ok
    end,
    if GetEnabled ->
        {ok, _GetPid} =
            ddfs_get:start([{port, GetPort}], {DdfsRoot, DiscoRoot});
    true ->
        ok
    end,

    spawn_link(fun() -> refresh_tags(DdfsRoot, Vol) end),
    spawn_link(fun() -> monitor_diskspace(DdfsRoot, Vol) end),

    {ok, #state{nodename = NodeName,
                root = DdfsRoot,
                volumes = [{0, V} || V <- Vol],
                tags = Tags,
                putq = http_queue:new(PutMax, ?HTTP_QUEUE_LENGTH),
                getq = http_queue:new(GetMax, ?HTTP_QUEUE_LENGTH)}}.

handle_call(get_tags, _, #state{tags = Tags} = S) ->
    {reply, gb_trees:keys(Tags), S};

handle_call(get_volumes, _, #state{volumes = Volumes, root = Root} = S) ->
    {reply, {Volumes, Root}, S};

handle_call(get_blob, {Pid, _Ref} = From, #state{getq = Q} = S) ->
    Reply = fun() -> gen_server:reply(From, ok) end,
    case http_queue:add({Pid, Reply}, Q) of
        full ->
            {reply, full, S};
        {_, NewQ} ->
            erlang:monitor(process, Pid),
            {noreply, S#state{getq = NewQ}}
    end;

handle_call({put_blob, BlobName}, {Pid, _Ref} = From, #state{putq = Q} = S) ->
    Reply = fun() ->
        Vol = choose_volume(S#state.volumes),
        {ok, Local, Url} = ddfs_util:hashdir(list_to_binary(BlobName),
            S#state.nodename, "blob", S#state.root, Vol),
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
        {value, {_Time, _Vol} = TagNfo} ->
            {reply, {ok, TagNfo}, S}
    end;

handle_call({get_tag_data, TagName, TagNfo}, From, S) ->
    spawn(fun() ->
        read_tag(TagName, S#state.nodename, S#state.root, TagNfo, From)
    end),
    {noreply, S};

handle_call({put_tag_data, {Tag, Data}}, _From, S) ->
    Vol = choose_volume(S#state.volumes),
    {ok, Local, _} = ddfs_util:hashdir(Tag, S#state.nodename,
        "tag", S#state.root, Vol),
    case ddfs_util:ensure_dir(Local) of
        ok ->
            F = filename:join(Local, ["!partial.", binary_to_list(Tag)]),
            {reply, case prim_file:write_file(F, Data) of
                ok -> {ok, Vol};
                {error, _} = E -> E
            end, S};
        E ->
            {reply, E, S}
    end;

handle_call({put_tag_commit, Tag, TagVol}, _, S) ->
    {value, {_, Vol}} = lists:keysearch(node(), 1, TagVol),
    {ok, Local, Url} = ddfs_util:hashdir(Tag, S#state.nodename,
        "tag", S#state.root, Vol),
    {TagName, Time} = ddfs_util:unpack_objname(Tag),

    TagL = binary_to_list(Tag),
    Src = filename:join(Local, ["!partial.", TagL]),
    Dst = filename:join(Local,  TagL),
    case ddfs_util:safe_rename(Src, Dst) of
        ok ->
            {reply, {ok, Url}, S#state{
                tags = gb_trees:enter(TagName, {Time, Vol}, S#state.tags)}};
        {error, _} = E ->
            {reply, E, S}
    end.

handle_cast({update_volumestats, NewVol}, #state{volumes = Vol} = S) ->
    {noreply, S#state{volumes = lists:ukeymerge(2, NewVol, Vol)}};

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

read_tag(Tag, NodeName, Root, {_, Vol}, From) ->
    {ok, D, _} = ddfs_util:hashdir(Tag, NodeName, "tag", Root, Vol),
    case prim_file:read_file(filename:join(D, binary_to_list(Tag))) of
        {ok, Bin} ->
            gen_server:reply(From, {ok, Bin});
        E ->
            error_logger:warning_report({"Read failed", Tag, D, E}),
            gen_server:reply(From, {error, read_failed})
    end.

init_volumes(Root, Volumes) ->
    lists:foreach(fun(Volume) ->
                          prim_file:make_dir(filename:join([Root, Volume, "blob"])),
                          prim_file:make_dir(filename:join([Root, Volume, "tag"]))
                  end, Volumes),
    {ok, Volumes}.

find_volumes(Root) ->
    case prim_file:list_dir(Root) of
        {ok, Files} ->
            case [F || "vol" ++ _ = F <- Files] of
                [] ->
                    Volume = "vol0",
                    prim_file:make_dir(filename:join([Root, Volume])),
                    error_logger:warning_report({"Could not find volumes in ", Root,
                                                 "Created ", Volume}),
                    init_volumes(Root, [Volume]);
                Volumes ->
                    init_volumes(Root, Volumes)
            end;
        Error ->
            error_logger:warning_report(
                {"Invalid root directory", Root, Error}),
            Error
    end.

find_tags(Root, Vols) ->
    {ok, lists:foldl(fun(Vol, Tags) ->
        ddfs_util:fold_files(filename:join([Root, Vol, "tag"]),
            fun(Tag, _, Tags1) ->
                parse_tag(Tag, Vol, Tags1)
            end,
        Tags)
    end, gb_trees:empty(), Vols)}.

parse_tag("!" ++ _, _, Tags) -> Tags;
parse_tag(Tag, Vol, Tags) ->
    {TagName, Time} = ddfs_util:unpack_objname(Tag),
    case gb_trees:lookup(TagName, Tags) of
        none ->
            gb_trees:insert(TagName, {Time, Vol}, Tags);
        {value, {OTime, _}} when OTime < Time ->
            gb_trees:enter(TagName, {Time, Vol}, Tags);
        _ ->
            Tags
    end.

choose_volume(Volumes) ->
    % Choose the volume with most available space
    [{_, Vol}|_] = lists:reverse(lists:keysort(1, Volumes)),
    Vol.

monitor_diskspace(Root, Vols) ->
    timer:sleep(?DISKSPACE_INTERVAL),
    gen_server:cast(ddfs_node, {update_volumestats,
        [{S, V} || {V, {ok, S}} <-
            [{V, ddfs_util:diskspace(filename:join(Root, V))} || V <- Vols]]}),
    monitor_diskspace(Root, Vols).

refresh_tags(Root, Vols) ->
    timer:sleep(?FIND_TAGS_INTERVAL),
    {ok, Tags} = find_tags(Root, Vols),
    gen_server:cast(ddfs_node, {update_tags, Tags}),
    refresh_tags(Root, Vols).


