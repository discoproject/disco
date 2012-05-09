
-module(disco_server).
-behaviour(gen_server).

-export([start_link/0]).
-export([update_config_table/3, get_active/1, get_nodeinfo/1, get_purged/1,
         new_job/3, kill_job/1, kill_job/2, purge_job/1, clean_job/1,
         new_task/2, connection_status/2, manual_blacklist/2, gc_blacklist/1,
         get_worker_jobpack/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/file.hrl").
-include("disco.hrl").
-include("gs_util.hrl").

-type connection_status() :: undefined | up | erlang:timestamp().

-record(dnode, {host :: host_name(),
                node_mon :: pid(),
                manual_blacklist :: boolean(),
                connection_status :: connection_status(),
                slots :: cores(),
                num_running :: cores(),
                stats_ok :: non_neg_integer(),
                stats_failed :: non_neg_integer(),
                stats_crashed :: non_neg_integer()}).
-type dnode() :: #dnode{}.

-record(state, {workers :: gb_tree(),
                nodes :: gb_tree(),
                purged :: gb_tree(),
                jobpack_queue :: pid()}).
-type state() :: #state{}.

-export_type([connection_status/0]).

-define(PURGE_TIMEOUT, 86400000). % 24h

% This controls the max number of simultaneously open file
% descriptors that can be open to service jobpack requestors.
-define(NUM_ACTIVE_JOBPACK_SENDERS, 1024).

% This specifies the maximum amount of time to wait for a completion
% ack from a jobpack requestor, in milliseconds.
-define(JOBPACK_HANDLE_TIMEOUT, 10 * 60 * 1000).

%% ===================================================================
%% API functions

-spec start_link() -> {ok, pid()}.
start_link() ->
    lager:info("DISCO SERVER STARTS"),
    case gen_server:start_link({local, ?MODULE}, ?MODULE,
                               [], disco:debug_flags("disco_server")) of
        {ok, Server} ->
            case catch disco_config:get_config_table() of
                {ok, _Config} ->
                    {ok, Server};
                E ->
                    lager:warning("Parsing config failed: ~p", [E])
            end;
        {error, {already_started, Server}} ->
            {ok, Server}
    end.

-spec update_config_table([disco_config:host_info()], [host_name()],
                          [host_name()]) -> ok.
update_config_table(Config, Blacklist, GCBlacklist) ->
    gen_server:cast(?MODULE,
                    {update_config_table, Config, Blacklist, GCBlacklist}).

-type active_tasks() :: [{host_name(), task()}].
-spec get_active(nonempty_string() | all) -> {ok, active_tasks()}.
get_active(JobName) ->
    gen_server:call(?MODULE, {get_active, JobName}).

-spec get_nodeinfo(all) -> {ok, [nodeinfo()]}.
get_nodeinfo(Spec) ->
    gen_server:call(?MODULE, {get_nodeinfo, Spec}).

-spec new_job(jobname(), pid(), non_neg_integer()) -> ok.
new_job(JobName, JobCoord, Timeout) ->
    gen_server:call(?MODULE, {new_job, JobName, JobCoord}, Timeout).

-spec kill_job(jobname()) -> ok.
kill_job(JobName) ->
    gen_server:call(?MODULE, {kill_job, JobName}).
-spec kill_job(jobname(), non_neg_integer()) -> ok.
kill_job(JobName, Timeout) ->
    gen_server:call(?MODULE, {kill_job, JobName}, Timeout).

-spec purge_job(jobname()) -> ok.
purge_job(JobName) ->
    gen_server:cast(?MODULE, {purge_job, JobName}).

-spec clean_job(jobname()) -> ok.
clean_job(JobName) ->
    gen_server:call(?MODULE, {clean_job, JobName}).

-spec new_task(task(), non_neg_integer()) -> ok | failed.
new_task(Task, Timeout) ->
    gen_server:call(?MODULE, {new_task, Task}, Timeout).

-spec connection_status(host_name(), up | down) -> ok.
connection_status(Node, Status) ->
    gen_server:call(?MODULE, {connection_status, Node, Status}).

-spec manual_blacklist(host_name(), boolean()) -> ok.
manual_blacklist(Node, True) ->
    gen_server:call(?MODULE, {manual_blacklist, Node, True}).

-spec gc_blacklist([host_name()]) -> ok.
gc_blacklist(Hosts) ->
    gen_server:call(?MODULE, {gc_blacklist, Hosts}).

% called from remote nodes
-spec get_worker_jobpack(node(), jobname())
                        -> {ok, {file:io_device(), non_neg_integer(), pid()}}.
get_worker_jobpack(Master, JobName) ->
    gen_server:call({?MODULE, Master}, {jobpack, JobName}).

-spec get_purged(node()) -> [binary()].
get_purged(Master) ->
    gen_server:call({?MODULE, Master}, get_purged).

%% ===================================================================
%% gen_server callbacks

-spec init(_) -> {ok, state()}.
init(_Args) ->
    process_flag(trap_exit, true),
    {ok, _} = fair_scheduler:start_link(),
    JobPackQ = work_queue:start_link(?NUM_ACTIVE_JOBPACK_SENDERS),
    {ok, #state{workers = gb_trees:empty(),
                nodes = gb_trees:empty(),
                purged = gb_trees:empty(),
                jobpack_queue = JobPackQ}}.

-type update_config_msg() :: {update_config_table, [disco_config:host_info()],
                              [host_name()], [host_name()]}.
-spec handle_cast(update_config_msg() | schedule_next | {purge_job, jobname()},
                  state()) -> gs_noreply().
handle_cast({update_config_table, Config, ManualBlacklist, GCBlacklist}, S) ->
    {noreply, do_update_config_table(Config, ManualBlacklist, GCBlacklist, S)};

handle_cast(schedule_next, S) ->
    {noreply, do_schedule_next(S)};

handle_cast({purge_job, JobName}, S) ->
    {noreply, do_purge_job(JobName, S)}.

-spec handle_call(dbg_state_msg(), from(), state()) -> gs_reply(state());
                 ({new_job, jobname(), pid()}, from(), state()) -> gs_reply(ok);
                 ({new_task, task()}, from(), state()) -> gs_reply(ok | failed);
                 ({get_active, jobname()}, from(), state()) ->
                         gs_reply({ok, active_tasks()});
                 ({get_nodeinfo, all}, from(), state()) ->
                         gs_reply({ok, [nodeinfo()]});
                 (get_purged, from(), state()) -> gs_reply({ok, [binary()]});
                 (get_num_cores, from(), state()) -> gs_reply({ok, cores()});
                 ({jobpack, jobname()}, from(), state()) -> gs_noreply();
                 ({kill_job, jobname()} | {clean_job, jobname()}
                  | connection_status | manual_blacklist | gc_blacklist,
                  from(), state()) -> gs_reply(ok).
handle_call(dbg_get_state, _, S) ->
    {reply, S, S};

handle_call({new_job, JobName, JobCoord}, _, S) ->
    {reply, do_new_job(JobName, JobCoord, S), S};

handle_call({new_task, Task}, _, S) ->
    {reply, do_new_task(Task, S), S};

handle_call({get_active, JobName}, _From, S) ->
    {reply, do_get_active(JobName, S), S};

handle_call({get_nodeinfo, all}, _From, S) ->
    {reply, do_get_nodeinfo(S), S};

handle_call(get_purged, _, S) ->
    {Result, S1} = do_get_purged(S),
    {reply, Result, S1};

handle_call(get_num_cores, _, S) ->
    {reply, do_get_num_cores(S), S};

handle_call({jobpack, JobName}, From, #state{jobpack_queue = Q} = S) ->
    Sender = fun() -> do_send_jobpack(JobName, From) end,
    work_queue:add_work(Q, Sender),
    {noreply, S};

handle_call({kill_job, JobName}, _From, S) ->
    {reply, do_kill_job(JobName), S};

handle_call({clean_job, JobName}, _From, S) ->
    {reply, do_clean_job(JobName), S};

handle_call({connection_status, Node, Status}, _From, S) ->
    {reply, ok, do_connection_status(Node, Status, S)};

handle_call({manual_blacklist, Node, True}, _From, S) ->
    {reply, ok, do_manual_blacklist(Node, True, S)};

handle_call({gc_blacklist, Hosts}, _From, S) ->
    {reply, ok, do_gc_blacklist(Hosts, S)}.


-spec handle_info({reference(), term()} | {'EXIT', pid(), term()}, state())
                 -> gs_noreply() | gs_stop(stop_requested).
% handle late replies to "catch gen_server:call"
handle_info({Ref, _Msg}, S) when is_reference(Ref) ->
    {noreply, S};

handle_info({'EXIT', Pid, Reason}, S) when Pid == self() ->
    lager:warning("Disco server dies on error: ~p", [Reason]),
    {stop, stop_requested, S};

handle_info({'EXIT', Pid, {shutdown, Results}}, S) ->
    process_exit(Pid, Results, S);

handle_info({'EXIT', Pid, noconnection}, S) ->
    process_exit(Pid, {error, "Connection lost to the node (network busy?)"}, S);

handle_info({'EXIT', Pid, {error, _} = Msg}, S) ->
    process_exit(Pid, Msg, S);

handle_info({'EXIT', Pid, Reason}, S) ->
    process_exit(Pid, {error, Reason}, S).

%% ===================================================================
%% gen_server callback stubs

-spec terminate(term(), state()) -> ok.
terminate(Reason, _State) ->
    lager:warning("Disco server dies: ~p", [Reason]).

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% ===================================================================
%% exit handlers

process_exit(Pid, {Type, _} = Results, S) ->
    case gb_trees:lookup(Pid, S#state.workers) of
        none ->
            nodemon_exit(Pid, S);
        {_, {Host, Task}} ->
            UWorkers = gb_trees:delete(Pid, S#state.workers),
            Task#task.from ! {Results, Task, Host},
            schedule_next(),
            {noreply, update_stats(Host,
                                   gb_trees:lookup(Host, S#state.nodes),
                                   Type,
                                   S#state{workers = UWorkers})}
    end.

-spec nodemon_exit(pid(), state()) -> gs_noreply().
nodemon_exit(Pid, S) ->
    Iter = gb_trees:iterator(S#state.nodes),
    nodemon_exit(Pid, S, gb_trees:next(Iter)).

nodemon_exit(Pid, S, {Host, #dnode{node_mon = Pid} = N, _Iter}) ->
    lager:warning("Restarting monitor for ~p", [Host]),
    N1 = N#dnode{node_mon = node_mon:start_link(Host)},
    S1 = S#state{nodes = gb_trees:update(Host, N1, S#state.nodes)},
    {noreply, do_connection_status(Host, down, S1)};

nodemon_exit(Pid, S, {_Host, _N, Iter}) ->
    nodemon_exit(Pid, S, gb_trees:next(Iter));

nodemon_exit(Pid, S, none) ->
    lager:warning("Unknown pid ~p exits", [Pid]),
    {noreply, S}.

%% ===================================================================
%% internal functions

-spec allow_write(#dnode{}) -> boolean().
allow_write(#dnode{connection_status = up,
                   manual_blacklist = false}) ->
    true;
allow_write(#dnode{}) ->
    false.

-spec allow_read(#dnode{}) -> boolean().
allow_read(#dnode{connection_status = up}) ->
    true;
allow_read(#dnode{}) ->
    false.

-spec allow_task(#dnode{}) -> boolean().
allow_task(#dnode{} = N) -> allow_write(N).

-spec update_nodes(gb_tree()) -> ok.
update_nodes(Nodes) ->
    WhiteNodes = [{N#dnode.host, N#dnode.slots}
                  || N <- gb_trees:values(Nodes), allow_task(N)],
    DDFSNodes = [{disco:slave_node(N#dnode.host), allow_write(N), allow_read(N)}
                 || N <- gb_trees:values(Nodes)],
    ddfs_master:update_nodes(DDFSNodes),
    gen_server:cast(scheduler, {update_nodes, WhiteNodes}),
    schedule_next().

-spec update_stats(host_name(), none | {value, dnode()}, _,
                   state()) -> state().
update_stats(_Node, none, _ReplyType, S) -> S;
update_stats(Node, {value, N}, ReplyType, S) ->
    M = N#dnode{num_running = N#dnode.num_running - 1},
    M0 = case ReplyType of
             done ->
                 M#dnode{stats_ok = M#dnode.stats_ok + 1};
             error ->
                 M#dnode{stats_failed = M#dnode.stats_failed + 1};
             fatal ->
                 M#dnode{stats_crashed = M#dnode.stats_crashed + 1};
             _ ->
                 M#dnode{stats_crashed = M#dnode.stats_crashed + 1}
         end,
    S#state{nodes = gb_trees:update(Node, M0, S#state.nodes)}.

-spec do_connection_status(host_name(), up | down, state()) -> state().
do_connection_status(Node, Status, #state{nodes = Nodes} = S) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            {value, N} when Status =:= up ->
                N1 = N#dnode{connection_status = up},
                gb_trees:update(Node, N1, Nodes);
            {value, N} when Status =:= down ->
                N1 = N#dnode{connection_status = now()},
                gb_trees:update(Node, N1, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    S#state{nodes = UpdatedNodes}.

-spec do_manual_blacklist(host_name(), boolean(), state()) -> state().
do_manual_blacklist(Node, True, #state{nodes = Nodes} = S) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            {value, N} ->
                N1 = N#dnode{manual_blacklist = True},
                gb_trees:update(Node, N1, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    S#state{nodes = UpdatedNodes}.

-spec do_update_config_table([disco_config:host_info()], [host_name()],
                             [host_name()], state()) -> state().
do_update_config_table(Config, Blacklist, GCBlacklist, S) ->
    lager:info("Config table updated"),
    NewNodes =
        lists:foldl(fun({Host, Slots}, NewNodes) ->
            NewNode =
                case gb_trees:lookup(Host, S#state.nodes) of
                    none ->
                        #dnode{host = Host,
                               node_mon = node_mon:start_link(Host),
                               manual_blacklist = lists:member(Host, Blacklist),
                               connection_status = undefined,
                               slots = Slots,
                               num_running = 0,
                               stats_ok = 0,
                               stats_failed = 0,
                               stats_crashed = 0};
                    {value, N} ->
                        N#dnode{slots = Slots,
                                manual_blacklist = lists:member(Host, Blacklist)}
                end,
            gb_trees:insert(Host, NewNode, NewNodes)
        end, gb_trees:empty(), Config),
    lists:foreach(
        fun(OldNode) ->
            case gb_trees:lookup(OldNode#dnode.host, NewNodes) of
                none ->
                    unlink(OldNode#dnode.node_mon),
                    exit(OldNode#dnode.node_mon, kill);
                _ -> ok
            end
        end, gb_trees:values(S#state.nodes)),
    disco_proxy:update_nodes(gb_trees:keys(NewNodes)),
    update_nodes(NewNodes),
    S1 = do_gc_blacklist(GCBlacklist, S),
    S1#state{nodes = NewNodes}.

-spec do_gc_blacklist([host_name()], state()) -> state().
do_gc_blacklist(Hosts, S) ->
    ddfs_master:gc_blacklist([disco:slave_node(H) || H <- Hosts]),
    S.

-spec start_worker(host_name(), pid(), task()) -> pid().
start_worker(Node, NodeMon, T) ->
    event_server:event(T#task.jobname, "~s:~B assigned to ~s",
                       [T#task.mode, T#task.taskid, Node], []),
    spawn_link(disco_worker, start_link_remote, [Node, NodeMon, T]).

-spec schedule_next() -> ok.
schedule_next() ->
    gen_server:cast(?MODULE, schedule_next).

-spec do_schedule_next(state()) -> state().
do_schedule_next(#state{nodes = Nodes, workers = Workers} = S) ->
    Running = [{Y, N} || #dnode{slots = X, num_running = Y, host = N} = Node
                             <- gb_trees:values(Nodes), X > Y, allow_task(Node)],
    {_, AvailableNodes} = lists:unzip(lists:keysort(1, Running)),
    if AvailableNodes =/= [] ->
        case gen_server:call(scheduler, {next_task, AvailableNodes}) of
            {ok, {JobSchedPid, {Node, Task}}} ->
                M = gb_trees:get(Node, Nodes),
                WorkerPid = start_worker(Node, M#dnode.node_mon, Task),
                UWorkers = gb_trees:insert(WorkerPid, {Node, Task}, Workers),
                gen_server:cast(JobSchedPid, {task_started, Node, WorkerPid}),

                M1 = M#dnode{num_running = M#dnode.num_running + 1},
                UNodes = gb_trees:update(Node, M1, Nodes),
                S1 = S#state{nodes = UNodes, workers = UWorkers},
                do_schedule_next(S1);
            nojobs ->
                S
        end;
       true -> S
    end.

-spec do_purge_job(jobname(), state()) -> state().
do_purge_job(JobName, #state{purged = Purged} = S) ->
    _ = handle_call({clean_job, JobName}, {self(), none}, S),
    % NB! next line disabled for 0.3.1, ISSUE #227
    %ddfs:delete(ddfs_master, disco:oob_name(JobName), internal),
    Key = list_to_binary(JobName),
    NPurged =
        case gb_trees:is_defined(Key, Purged) of
            true ->
                Purged;
            false ->
                gb_trees:insert(Key, now(), Purged)
        end,
    S#state{purged = NPurged}.


-spec do_new_job(jobname(), pid(), state()) -> ok.
do_new_job(JobName, JobCoord, _S) ->
    catch gen_server:call(scheduler, {new_job, JobName, JobCoord}).

-spec do_new_task(task(), state()) -> ok | failed.
do_new_task(Task, S) ->
    NodeStats = [case gb_trees:lookup(Node, S#state.nodes) of
                     none -> {false, Input};
                     {value, N} -> {N#dnode.num_running, Input}
                 end || {_Url, Node} = Input <- Task#task.input],
    case catch gen_server:call(scheduler, {new_task, Task, NodeStats}) of
        ok ->
            schedule_next(),
            ok;
        unknown_job ->
            % most likely job was killed
            failed
    end.

-spec do_get_active(jobname() | all, state()) -> {ok, active_tasks()}.
do_get_active(all, #state{workers = Workers}) ->
    {ok, gb_trees:values(Workers)};
do_get_active(JobName, #state{workers = Workers}) ->
    Active = [{Host, Task} || {Host, #task{jobname = N} = Task}
                                  <- gb_trees:values(Workers), N == JobName],
    {ok, Active}.

-spec do_get_nodeinfo(state()) -> {ok, [nodeinfo()]}.
do_get_nodeinfo(#state{nodes = Nodes}) ->
    Info = [#nodeinfo{name = N#dnode.host,
                      slots = N#dnode.slots,
                      num_running = N#dnode.num_running,
                      stats_ok = N#dnode.stats_ok,
                      stats_failed = N#dnode.stats_failed,
                      stats_crashed = N#dnode.stats_crashed,
                      connected = N#dnode.connection_status =:= up,
                      blacklisted = N#dnode.manual_blacklist}
            || N <- gb_trees:values(Nodes)],
    {ok, Info}.

-spec do_get_purged(state()) -> {{ok, [binary()]}, state()}.
do_get_purged(#state{purged = Purged} = S) ->
    Now = now(),
    NPurgedList =
        [{Job, TStamp} || {Job, TStamp} <- gb_trees:to_list(Purged),
                          timer:now_diff(Now, TStamp) < ?PURGE_TIMEOUT * 1000],
    NPurged = gb_trees:from_orddict(NPurgedList),
    {{ok, gb_trees:keys(NPurged)}, S#state{purged = NPurged}}.

-spec do_get_num_cores(state()) -> {ok, cores()}.
do_get_num_cores(#state{nodes = Nodes}) ->
    {ok, lists:sum([N#dnode.slots || N <- gb_trees:values(Nodes)])}.

-spec do_kill_job(jobname()) -> ok.
do_kill_job(JobName) ->
    event_server:event(JobName, "WARN: Job killed", [], []),
    % Make sure that scheduler don't accept new tasks from this job
    gen_server:cast(scheduler, {job_done, JobName}),
    ok.

-spec do_clean_job(jobname()) -> ok.
do_clean_job(JobName) ->
    do_kill_job(JobName),
    gen_server:cast(event_server, {clean_job, JobName}),
    ok.

% This is executed in its own process.
-spec do_send_jobpack(jobname(), {pid(), reference()}) -> ok.
do_send_jobpack(JobName, From) ->
    JobFile = jobpack:jobfile(disco:jobhome(JobName)),
    case prim_file:read_file_info(JobFile) of
        {ok, #file_info{size = Size}} ->
            {ok, File} = file:open(JobFile, [binary, read]),
            {Worker, _Tag} = From,
            erlang:monitor(process, Worker),
            gen_server:reply(From, {ok, {File, Size, self()}}),
            receive
                done -> ok;
                {'DOWN', _, _, _, _} -> ok
            after ?JOBPACK_HANDLE_TIMEOUT ->
                ok
            end,
            file:close(File);
        {error, _} = E ->
            gen_server:reply(From, E)
    end.
