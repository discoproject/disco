-module(ddfs_node).
-behaviour(gen_server).

-export([start_link/1, stop/0, init/1, handle_call/3, handle_cast/2,
        handle_info/2, terminate/2, code_change/3]).

-include("config.hrl").

-record(state, {root, volumes, put_max, put_active, get_max, get_active, tags}).

start_link(Config) ->
    process_flag(trap_exit, true),
    error_logger:info_report([{"DDFS node starts"}]),
    case gen_server:start_link({local, ddfs_node}, ddfs_node, Config, []) of
        {ok, _Server} -> ok;
        {error, {already_started, _Server}} -> ok
    end,
    receive
        {'EXIT', _, Reason} ->
            exit(Reason)
    end.

stop() ->
    gen_server:call(ddfs_node, stop).

init(Config) ->
    {root, Root} = proplists:lookup(root, Config),
    {put_max, PutMax} = proplists:lookup(put_max, Config),
    {get_max, GetMax} = proplists:lookup(get_max, Config),
    {put_port, PutPort} = proplists:lookup(put_port, Config),
    {get_port, GetPort} = proplists:lookup(get_port, Config),

    {ok, VolUn} = find_volumes(Root),
    % lists:ukeymerge in update_volumestats requires a sorted list
    Vol = lists:sort(VolUn),
    error_logger:info_report({"found vols", Vol}), 
    {ok, Tags} = find_tags(Root, Vol),
    error_logger:info_report({"found tags", Tags}), 
    {ok, _PutPid} = ddfs_put:start([{port, PutPort}]),
    {ok, _GetPid} = ddfs_get:start([{port, GetPort}], Root),

    spawn_link(fun() -> refresh_tags(Root, Vol) end),
    spawn_link(fun() -> monitor_diskspace(Root, Vol) end),
    
    {ok, #state{root = Root,
                volumes = [{0, V} || V <- Vol],
                tags = Tags,
                put_max = PutMax,
                put_active = [],
                get_max = GetMax,
                get_active = []}}.

handle_call(get_tags, _, #state{tags = Tags} = S) ->
    {reply, gb_trees:keys(Tags), S};

handle_call(get_volumes, _, #state{volumes = Volumes, root = Root} = S) ->
    {reply, {Volumes, Root}, S};

handle_call(get_blob, _, #state{get_max = Max, get_active = Act} = S)
        when length(Act) >= Max ->
    {reply, busy, S};

handle_call(get_blob, {Pid, _}, S) ->
    erlang:monitor(process, Pid),
    {reply, ok, S#state{get_active = [Pid|S#state.get_active]}}; 

handle_call({put_blob, _}, _, #state{put_max = Max, put_active = Act} = S)
        when length(Act) >= Max ->
    {reply, busy, S};
        
handle_call({put_blob, BlobName}, {Pid, _}, S) ->
    Vol = choose_volume(S#state.volumes),
    {ok, Local, Url} = ddfs_util:hashdir(
        list_to_binary(BlobName), "blob", S#state.root, Vol),
    case ddfs_util:ensure_dir(Local) of 
        ok ->
            erlang:monitor(process, Pid),
            {reply, {ok, Local, Url},
                S#state{put_active = [Pid|S#state.put_active]}};
        {error, E} ->
            {reply, {error, Local, E}, S}
    end;

handle_call({get_tag_timestamp, TagName}, _From, S) ->
    case gb_trees:lookup(TagName, S#state.tags) of
        none ->
            {reply, notfound, S};
        {value, {_Time, _Vol} = TagNfo} ->
            {reply, {ok, TagNfo}, S}
    end;

handle_call({get_tag_data, TagName, TagNfo}, From, S) ->
    spawn(fun() -> read_tag(TagName, S#state.root, TagNfo, From) end),
    {noreply, S};

handle_call({put_tag_data, {Tag, Data}}, _From, S) ->
    Vol = choose_volume(S#state.volumes),
    {ok, Local, _} = ddfs_util:hashdir(Tag, "tag", S#state.root, Vol),
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
    error_logger:info_report({"put_tag_commit", Tag, TagVol}),
    {value, {_, Vol}} = lists:keysearch(node(), 1, TagVol),
    {ok, Local, Url} = ddfs_util:hashdir(Tag, "tag", S#state.root, Vol),
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

handle_info({'DOWN', _, _, Pid, _}, S) ->
    case S#state.put_active -- [Pid] of
        L when L == S#state.put_active ->
            {noreply, S#state{get_active = S#state.get_active -- [Pid]}};
        L ->
            {noreply, S#state{put_active = L}}
    end.

% callback stubs
terminate(_Reason, _State) -> {}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

read_tag(Tag, Root, {_, Vol}, From) ->
    {ok, D, _} = ddfs_util:hashdir(Tag, "tag", Root, Vol),
    case prim_file:read_file(filename:join(D, binary_to_list(Tag))) of
        {ok, Bin} ->
            gen_server:reply(From, {ok, Bin});
        _ ->
            gen_server:reply(From, {error, read_failed})
    end.

find_volumes(Root) ->
    case prim_file:list_dir(Root) of
        {ok, Files} ->
            case [F || "vol" ++ _ = F <- Files] of
                [] ->
                    error_logger:warning_report(
                        {"Could not find volumes in", Root}),
                    {error, novolumes};
                Vol ->
                    lists:foreach(fun(V) ->
                        prim_file:make_dir(filename:join([Root, V, "blob"])),
                        prim_file:make_dir(filename:join([Root, V, "tag"]))
                    end, Vol),
                    {ok, Vol}
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


