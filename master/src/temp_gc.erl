-module(temp_gc).

-include_lib("kernel/include/file.hrl").

-export([start_link/5]).

-define(GC_INTERVAL, 600000).

start_link(Master, EventServer, DataRoot, Node, GCAfter) ->
    Root = filename:join(DataRoot, Node),
    case catch {gen_server:call(Master, get_purged),
                gen_server:call(EventServer, get_jobs)} of
        {{ok, Purged}, {ok, Jobs}} ->
            {ok, Dirs} = prim_file:list_dir(Root),
            Active = gb_sets:from_list(
                [Name || {Name, active, _Start, _Pid} <- Jobs]),
            process_dir(Dirs, Root, GCAfter, Purged, Active);
        _ ->
            % master busy, try again after GC_INTERVAL
            ok
    end,
    timer:sleep(?GC_INTERVAL),
    start_link(Master, EventServer, DataRoot, Node, GCAfter).

process_dir([], _Root, _GCAfter, _Purged, _Active) -> ok;
process_dir([Dir|R], Root, GCAfter, Purged, Active) ->
    Path = filename:join(Root, Dir),
    {ok, Jobs} = prim_file:list_dir(Path),
    [process_job(filename:join(Path, Job), GCAfter, Purged) ||
        Job <- Jobs, ifdead(Job, Active)],
    process_dir(R, Root, GCAfter, Purged, Active).

ifdead(Job, Active) ->
    not gb_sets:is_member(list_to_binary(Job), Active).

process_job(Job, GCAfter, Purged) ->
    case prim_file:read_file_info(Job) of
        {ok, #file_info{type = directory, mtime = TStamp}} ->
            T = calendar:datetime_to_gregorian_seconds(TStamp),
            Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
            IsPurged = gb_sets:is_member(
                list_to_binary(filename:basename(Job)), Purged),
            if IsPurged; Now - T > GCAfter ->
                os:cmd("rm -Rf " ++ Job);
            true ->
                ok
            end;
        _ ->
            ok
    end.

