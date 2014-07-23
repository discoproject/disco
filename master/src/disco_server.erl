-module(disco_server).
-behaviour(gen_server).

-export([start_link/0]).
-export([update_config_table/3, get_active/1, get_nodeinfo/1, get_purged/1,
         new_job/3, kill_job/1, kill_job/2, purge_job/1, clean_job/1,
         new_task/1, connection_status/2, manual_blacklist/2, gc_blacklist/1,
         get_worker_jobpack/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/file.hrl").

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-type connection_status() :: undefined | {up | down, erlang:timestamp()}.

-record(dnode, {host :: host(),
                node_mon :: pid(),
                manual_blacklist :: boolean(),
                connection_status :: connection_status(),
                slots :: cores(),
                num_running :: cores(),
                stats_ok :: non_neg_integer(),
                stats_failed :: non_neg_integer(),
                stats_crashed :: non_neg_integer()}).
-type dnode() :: #dnode{}.

-record(state, {workers = gb_trees:empty() :: disco_gbtree(pid(), {host(), task()}),
                nodes   = gb_trees:empty() :: disco_gbtree(host(), dnode()),
                purged  = gb_trees:empty() :: disco_gbtree(binary(), erlang:timestamp()),
                jobpack_queue :: pid(),

                % The below are only used in cluster-in-a-box mode.
                % The main purpose is to track the get/put port
                % assignments for the simulated hosts.  Get/Put ports
                % are allocated sequentially, starting from
                % DISCO_PORT.
                port_map  :: port_map()}).
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
            try
                {ok, _Config} = disco_config:get_config_table(),
                {ok, Server}
            catch K:E -> lager:warning("Parsing config failed: ~p:~p", [K, E])
            end;
        {error, {already_started, Server}} ->
            {ok, Server}
    end.

-spec update_config_table([disco_config:host_info()], [host()], [host()]) -> ok.
update_config_table(Config, Blacklist, GCBlacklist) ->
    gen_server:cast(?MODULE,
                    {update_config_table, Config, Blacklist, GCBlacklist}).

-type active_tasks() :: [{host(), jobname(), stage_name()}].
-spec get_active(jobname() | all) -> {ok, active_tasks()}.
get_active(JobName) ->
    gen_server:call(?MODULE, {get_active, JobName}).

-spec get_nodeinfo(all) -> {ok, [nodeinfo()]}.
get_nodeinfo(Spec) ->
    gen_server:call(?MODULE, {get_nodeinfo, Spec}).

-spec new_job(jobname(), pid(), non_neg_integer()) -> ok | {error, term()}.
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

-spec new_task(task()) -> ok.
new_task(Task) ->
    gen_server:cast(?MODULE, {new_task, Task, self()}).

-spec connection_status(host(), up | down) -> ok.
connection_status(Node, Status) ->
    gen_server:call(?MODULE, {connection_status, Node, Status}).

-spec manual_blacklist(host(), boolean()) -> ok.
manual_blacklist(Node, True) ->
    gen_server:call(?MODULE, {manual_blacklist, Node, True}).

-spec gc_blacklist([host()]) -> ok.
gc_blacklist(Hosts) ->
    gen_server:call(?MODULE, {gc_blacklist, Hosts}).

% called from remote nodes
-spec get_worker_jobpack(node(), jobname())
                        -> {ok, {file:io_device(), non_neg_integer(), pid()}}.
get_worker_jobpack(Master, JobName) ->
    gen_server:call({?MODULE, Master}, {jobpack, JobName}).

-spec get_purged(node()) -> {ok, [binary()]}.
get_purged(Master) ->
    gen_server:call({?MODULE, Master}, get_purged).

%% ===================================================================
%% gen_server callbacks

-spec init(_) -> gs_init().
init(_Args) ->
    process_flag(trap_exit, true),
    {ok, _} = fair_scheduler:start_link(),
    JobPackQ = work_queue:start_link(?NUM_ACTIVE_JOBPACK_SENDERS),
    PortMap = case disco:local_cluster() of
                  true -> {list_to_integer(disco:get_setting("DISCO_PORT")) + 2,
                           gb_trees:empty()};
                  false -> none
              end,
    {ok, #state{jobpack_queue = JobPackQ, port_map = PortMap}}.

-type update_config_msg() :: {update_config_table, [disco_config:host_info()],
                              [host()], [host()]}.
-spec handle_cast(update_config_msg() | schedule_next | {purge_job, jobname()},
                  state()) -> gs_noreply();
                 ({new_task, task(), pid()}, state()) -> gs_noreply().
handle_cast({update_config_table, Config, ManualBlacklist, GCBlacklist}, S) ->
    {noreply, do_update_config_table(Config, ManualBlacklist, GCBlacklist, S)};

handle_cast(schedule_next, S) ->
    {noreply, do_schedule_next(S)};

handle_cast({new_task, Task, JC}, S) ->
    do_new_task(Task, JC, S),
    {noreply, S};

handle_cast({purge_job, JobName}, S) ->
    {noreply, do_purge_job(JobName, S)}.

-spec handle_call(dbg_state_msg(), from(), state()) -> gs_reply(state());
                 ({new_job, jobname(), pid()}, from(), state()) -> gs_reply(ok);
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

process_exit(Pid, {Type, _} = Results, #state{workers = Workers,
                                              nodes = Nodes} = S) ->
    case gb_trees:lookup(Pid, Workers) of
        none ->
            nodemon_exit(Pid, S);
        {value, {H, {#task_spec{job_coord = C, taskid = Id}, #task_run{}}}} ->
            job_coordinator:task_done(C, {Results, Id, H}),
            UWorkers = gb_trees:delete(Pid, Workers),
            schedule_next(),
            {noreply, update_stats(H,
                                   gb_trees:lookup(H, Nodes),
                                   Type,
                                   S#state{workers = UWorkers})}
    end.

-spec nodemon_exit(pid(), state()) -> gs_noreply().
nodemon_exit(Pid, #state{nodes = Nodes} = S) ->
    Iter = gb_trees:iterator(Nodes),
    nodemon_exit(Pid, S, gb_trees:next(Iter)).

nodemon_exit(Pid, #state{nodes = Nodes, port_map = PortMap} = S,
             {Host, #dnode{node_mon = Pid} = N, _Iter}) ->
    lager:warning("Restarting monitor for ~p", [Host]),
    N1 = N#dnode{node_mon = node_mon:start_link(Host, node_ports(Host, PortMap))},
    S1 = S#state{nodes = gb_trees:update(Host, N1, Nodes)},
    {noreply, do_connection_status(Host, down, S1)};

nodemon_exit(Pid, S, {_Host, _N, Iter}) ->
    nodemon_exit(Pid, S, gb_trees:next(Iter));

nodemon_exit(Pid, S, none) ->
    lager:warning("Unknown pid ~p exits", [Pid]),
    {noreply, S}.

%% ===================================================================
%% internal functions

-spec node_ports(host(), port_map()) -> node_ports().
node_ports(_Host, none) ->
    GetPort = list_to_integer(disco:get_setting("DISCO_PORT")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    #node_ports{get_port = GetPort, put_port = PutPort};
node_ports(Host, {_NextPort, PortMap}) ->
    {GetPort, PutPort} = gb_trees:get(Host, PortMap),
    #node_ports{get_port = GetPort, put_port = PutPort}.

-spec allow_write(dnode()) -> boolean().
allow_write(#dnode{connection_status = {up, _},
                   manual_blacklist = false}) ->
    true;
allow_write(#dnode{}) ->
    false.

-spec allow_read(dnode()) -> boolean().
allow_read(#dnode{connection_status = {up, _}}) ->
    true;
allow_read(#dnode{}) ->
    false.

-spec allow_task(dnode()) -> boolean().
allow_task(#dnode{} = N) -> allow_write(N).

-spec update_nodes(disco_gbtree(host(), dnode())) -> ok.
update_nodes(Nodes) ->
    WhiteNodes = [{H, S, R}
                  || #dnode{host = H, slots = S, num_running = R}
                         = N <- gb_trees:values(Nodes),
                     allow_task(N)],
    DDFSNodes = [{disco:slave_node(N#dnode.host),
                  allow_write(N), allow_read(N)} || N <- gb_trees:values(Nodes)],
    event_server:update_nodes([H || {H, _S, _R} <- WhiteNodes]),
    ddfs_master:update_nodes(DDFSNodes),
    fair_scheduler:update_nodes(WhiteNodes),
    schedule_next().

-spec update_stats(host(), none | {value, dnode()}, _, state()) -> state().
update_stats(_Node, none, _ReplyType, S) -> S;
update_stats(Node, {value, #dnode{num_running = NumRunning,
                                  stats_ok = StatsOk,
                                  stats_failed = StatsFailed,
                                  stats_crashed = StatsCrashed} = N},
             ReplyType, #state{nodes = Nodes} = S) ->
    M = N#dnode{num_running = NumRunning - 1},
    M0 = case ReplyType of
             done ->
                 M#dnode{stats_ok = StatsOk + 1};
             error ->
                 M#dnode{stats_failed = StatsFailed + 1};
             input_error ->
                 M#dnode{stats_failed = StatsFailed + 1};
             fatal ->
                 M#dnode{stats_crashed = StatsCrashed + 1};
             _ ->
                 M#dnode{stats_crashed = StatsCrashed + 1}
         end,
    S#state{nodes = gb_trees:update(Node, M0, Nodes)}.

-spec do_connection_status(host(), up | down, state()) -> state().
do_connection_status(Node, Status, #state{nodes = Nodes} = S) ->
    UpdatedNodes =
        case gb_trees:lookup(Node, Nodes) of
            {value, N} when Status =:= up ->
                N1 = N#dnode{connection_status = {up, now()}},
                gb_trees:update(Node, N1, Nodes);
            {value, N} when Status =:= down ->
                N1 = N#dnode{connection_status = {down, now()}},
                gb_trees:update(Node, N1, Nodes);
            _ -> Nodes
        end,
    update_nodes(UpdatedNodes),
    S#state{nodes = UpdatedNodes}.

-spec do_manual_blacklist(host(), boolean(), state()) -> state().
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

-spec init_port_map(port_map()) -> port_map().
init_port_map(none) -> none;
init_port_map({NextPort, _OldMap}) -> {NextPort, gb_trees:empty()}.

-spec update_port_map(port_map(), host()) -> port_map().
update_port_map(none, _Host) ->
    none;
update_port_map({NextPort, Map}, Host) ->
    GetPort = NextPort,
    PutPort = NextPort + 1,
    {NextPort + 2, gb_trees:insert(Host, {GetPort, PutPort}, Map)}.
-spec update_port_map(host(), port_map(), port_map()) -> port_map().
update_port_map(_Host, none, none) ->
    none;
update_port_map(Host, {_, OldMap}, {NP, CurMap}) ->
    {NP, gb_trees:insert(Host, gb_trees:get(Host, OldMap), CurMap)}.

-spec do_update_config_table([disco_config:host_info()], [host()],
                             [host()], state()) -> state().
do_update_config_table(Config, Blacklist, GCBlacklist,
                       #state{nodes = Nodes, port_map = OldPortMap} = S) ->
    lager:info("Config table updated"),
    {NewNodes, NewPortMap} =
        lists:foldl(
          fun({Host, Slots}, {NewNodes, PortMap}) ->
                  {NewNode, NewMap} =
                      case gb_trees:lookup(Host, Nodes) of
                          none ->
                              NewPortMap = update_port_map(PortMap, Host),
                              NodePorts = node_ports(Host, NewPortMap),
                              lager:debug("Adding new node ~p", [Host]),
                              {#dnode{host = Host,
                                      node_mon = node_mon:start_link(Host, NodePorts),
                                      manual_blacklist = lists:member(Host, Blacklist),
                                      connection_status = {down, now()},
                                      slots = Slots,
                                      num_running = 0,
                                      stats_ok = 0,
                                      stats_failed = 0,
                                      stats_crashed = 0},
                               NewPortMap};
                          {value, N} ->
                              {N#dnode{slots = Slots,
                                       manual_blacklist = lists:member(Host, Blacklist)},
                               update_port_map(Host, OldPortMap, PortMap)}
                      end,
                  {gb_trees:insert(Host, NewNode, NewNodes), NewMap}
          end, {gb_trees:empty(), init_port_map(OldPortMap)}, Config),
    lists:foreach(
      fun(#dnode{host = Host, node_mon = NodeMon}) ->
              case gb_trees:lookup(Host, NewNodes) of
                  none ->
                      lager:debug("Discarding node ~p", [Host]),
                      unlink(NodeMon),
                      exit(NodeMon, kill);
                  _ -> ok
              end
      end, gb_trees:values(Nodes)),
    disco_proxy:update_nodes(gb_trees:keys(NewNodes), NewPortMap),
    update_nodes(NewNodes),
    S1 = do_gc_blacklist(GCBlacklist, S),
    S1#state{nodes = NewNodes, port_map = NewPortMap}.

-spec do_gc_blacklist([host()], state()) -> state().
do_gc_blacklist(Hosts, S) ->
    ddfs_master:gc_blacklist([disco:slave_node(H) || H <- Hosts]),
    S.

-spec start_worker(host(), pid(), task()) -> pid().
start_worker(H, NM, {#task_spec{jehandler = JEHandler, stage = S, taskid = TI}, _} = T) ->
    job_event:event(JEHandler, "master", "~s:~B assigned to ~s", [S, TI, H], none),
    spawn_link(disco_worker, start_link_remote, [H, NM, T]).

-spec schedule_next() -> ok.
schedule_next() ->
    gen_server:cast(?MODULE, schedule_next).

-spec do_schedule_next(state()) -> state().
do_schedule_next(#state{nodes = Nodes, workers = Workers} = S) ->
    Running = [{Y, N} || #dnode{slots = X, num_running = Y, host = N} = Node
                             <- gb_trees:values(Nodes), X > Y, allow_task(Node)],
    {_, AvailableNodes} = lists:unzip(lists:keysort(1, Running)),
    if AvailableNodes =/= [] ->
        try fair_scheduler:next_task(AvailableNodes) of
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
        catch K:{timeout, E} ->
            lager:error("fair_scheduler timed out: ~p:~p", [K, E]),
            do_schedule_next(S)
        end;
       true -> S
    end.

-spec do_purge_job(jobname(), state()) -> state().
do_purge_job(JobName, #state{purged = Purged} = S) ->
    _ = handle_call({clean_job, JobName}, {self(), none}, S),
    Key = list_to_binary(JobName),
    NPurged =
        case gb_trees:is_defined(Key, Purged) of
            true ->
                Purged;
            false ->
                gb_trees:insert(Key, now(), Purged)
        end,
    S#state{purged = NPurged}.


-spec do_new_job(jobname(), pid(), state()) -> ok | {error, term()}.
do_new_job(JobName, JobCoord, _S) ->
    try fair_scheduler:new_job(JobName, JobCoord)
    catch K:V -> {error, {K, V}}
    end.

-spec do_new_task(task(), pid(), state()) -> ok.
do_new_task({#task_spec{jobname = Job, taskid = TaskId},
             #task_run{input = Inputs}} = Task, JC,
            #state{nodes = Nodes}) ->
    Locations = [pipeline_utils:locations(D) || {_Id, D} <- Inputs],
    LoadStats = lists:foldl(
                  fun(Host, Load) ->
                          case gb_trees:lookup(Host, Nodes) of
                              none ->
                                  Load;
                              {value, #dnode{num_running = Running}} ->
                                  gb_trees:enter(Host, Running, Load)
                          end
                  end, gb_trees:empty(), lists:flatten(Locations)),
    try case fair_scheduler:new_task(Task, LoadStats) of
            ok ->
                schedule_next(),
                ok;
            unknown_job ->
                job_coordinator:kill_job("unknown job", JC)
        end
    catch K:V ->
            lager:warning("Call to schedule task ~p of job ~p failed: ~p:~p",
                          [TaskId, Job, K, V]),
            job_coordinator:kill_job("Scheduling Error", JC)
    end.

-spec do_get_active(jobname() | all, state()) -> {ok, active_tasks()}.
do_get_active(all, #state{workers = W}) ->
    Active = [{H, J, S} || {H, {#task_spec{jobname = J, stage = S}, _}}
                               <- gb_trees:values(W)],
    {ok, Active};
do_get_active(JobName, #state{workers = W}) ->
    Active = [{H, JobName, S} || {H, {#task_spec{jobname = N, stage = S}, _}}
                                     <- gb_trees:values(W), N == JobName],
    {ok, Active}.

-spec do_get_nodeinfo(state()) -> {ok, [nodeinfo()]}.
do_get_nodeinfo(#state{nodes = Nodes}) ->
    Info = [#nodeinfo{name = Host,
                      slots = Slots,
                      num_running = NumRunning,
                      stats_ok = StatsOk,
                      stats_failed = StatsFailed,
                      stats_crashed = StatsCrashed,
                      connected = ConnectionStatus =:= up,
                      blacklisted = Blacklisted}
            || #dnode{host = Host,
                      slots = Slots,
                      num_running = NumRunning,
                      stats_ok = StatsOk,
                      stats_failed = StatsFailed,
                      stats_crashed = StatsCrashed,
                      connection_status = {ConnectionStatus, _},
                      manual_blacklist = Blacklisted} <- gb_trees:values(Nodes)],
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
    {ok, lists:sum([Slots || #dnode{slots = Slots} <- gb_trees:values(Nodes)])}.

-spec do_kill_job(jobname()) -> ok.
do_kill_job(JobName) ->
    event_server:event(JobName, "WARN: Job killed", [], none),
    % Make sure that scheduler don't accept new tasks from this job
    fair_scheduler:job_done(JobName),
    ok.

-spec do_clean_job(jobname()) -> ok.
do_clean_job(JobName) ->
    do_kill_job(JobName),
    event_server:clean_job(JobName),
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
