-module(disco_worker).
-behaviour(gen_server).

-export([start_link/1, start_link_remote/5]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

-include("task.hrl").
-record(state, {id, master, port, task,
        start_time,
        eventserver,
        eventstream,
        child_pid, node,
        linecount, errlines,
        results,
        debug,
        last_event, event_counter}).

-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 25).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).

-define(CMD, "nice -n 19 $DISCO_WORKER '~s' '~s' '~s' '~w' ~s").

port_options() ->
    [{line, 100000}, binary, exit_status,
     use_stdio, stderr_to_stdout,
     {env, [{"LD_LIBRARY_PATH", "lib"}, {"LC_ALL", "C"}] ++
      [{Setting, disco:get_setting(Setting)} || Setting <- disco:settings()]}].

start_link_remote(Master, Eventserver, Node, NodeMon, Task) ->
    Debug = disco:get_setting("DISCO_DEBUG") =/= "off",
    NodeAtom = node_mon:slave_node(Node),
    wait_until_node_ready(NodeMon),
    spawn_link(NodeAtom, disco_worker, start_link,
           [[self(), Eventserver, Master, Task, Node, Debug]]),
    process_flag(trap_exit, true),
    receive
        ok -> ok;
        {'EXIT', _, Reason} ->
            exit(Reason);
        _ ->
            exit({error, invalid_reply})
    after 60000 ->
        exit({worker_dies, {"Worker did not start in 60s", []}})
    end,
    wait_for_exit().

wait_until_node_ready(NodeMon) ->
    NodeMon ! {is_ready, self()},
    receive
        node_ready -> ok
    after 30000 ->
        exit({worker_dies, {"Node unavailable", []}})
    end.

wait_for_exit() ->
    receive
        {'EXIT', _, Reason} ->
            exit(Reason)
    end.

start_link([Parent|_] = Args) ->
    process_flag(trap_exit, true),
    Worker = case catch gen_server:start_link(disco_worker, Args, []) of
             {ok, Server} ->
                 Server;
             Reason ->
                 exit({worker_dies, {"Worker initialization failed: ~p",
                             [Reason]}})
         end,
    % NB: start_worker call is known to timeout if the node is really
    % busy - it should not be a fatal problem
    case catch gen_server:call(Worker, start_worker, 30000) of
        ok ->
            Parent ! ok;
        Reason1 ->
            exit({worker_dies, {"Worker startup failed: ~p",
                [Reason1]}})
    end,
    wait_for_exit().

init([Id, EventServer, Master, Task, Node, Debug]) ->
    process_flag(trap_exit, true),
    erlang:monitor(process, Task#task.from),
    {ok, #state{id = Id,
            master = Master,
            task = Task,
            node = Node,
            child_pid = none,
            eventserver = EventServer,
            eventstream = event_stream:new(),
            linecount = 0,
            start_time = now(),
            last_event = now(),
            event_counter = 0,
            errlines = message_buffer:new(?ERRLINES_MAX),
            debug = Debug,
            results = []}}.

spawn_cmd(#state{task = T, node = Node}) ->
    Args = [T#task.mode, T#task.jobname, Node, T#task.taskid, T#task.chosen_input],
    lists:flatten(io_lib:fwrite(?CMD, Args)).

worker_exit(#state{id = Id, master = Master}, Msg) ->
    gen_server:cast(Master, {exit_worker, Id, Msg}),
    normal.

event(Event, S) ->
    event(Event, S, []).

event({_Type, Message}, #state{task = T,
        eventserver = EventServer, node = Node}, Params) ->
    event_server:event(EventServer, Node, T#task.jobname, "[~s:~B] ~s",
        [T#task.mode, T#task.taskid, Message], Params).

error(State) ->
    {stop, worker_exit(State, {job_error, "Worker killed"}), State}.

error(Reason, State) ->
    error(nonrecoverable, Reason, State).

error(recoverable, Reason, State) ->
    Task = State#state.task,
    event({"WARN", Reason}, State, {task_failed, Task#task.mode}),
    {stop, worker_exit(State, {data_error, Reason}), State};
error(nonrecoverable, Reason, State) ->
    event({"ERROR", Reason}, State),
    {stop, worker_exit(State, {job_error, Reason}), State}.

handle_call(start_worker, _From, S) ->
    Cmd = spawn_cmd(S),
    if
        S#state.debug ->
            error_logger:info_report(["Spawn cmd: ", Cmd]);
        true ->
            ok
    end,
    Port = open_port({spawn, Cmd}, port_options()),
    {reply, ok, S#state{port = Port}, 30000}.

handle_cast(kill_worker, S) ->
    error(S).

handle_event({event, {<<"DAT">>, _Time, _Tags, Message}}, S) ->
    error(recoverable, Message, S);

handle_event({event, {<<"END">>, _Time, _Tags, _Message}}, S) ->
    event({"END", "Task finished in " ++ disco_server:format_time(S#state.start_time)}, S),
    {stop, worker_exit(S, {job_ok, S#state.results}), S};

handle_event({event, {<<"ERR">>, _Time, _Tags, Message}}, S) ->
    error(Message, S);

handle_event({event, {<<"PID">>, _Time, _Tags, ChildPID}}, S) ->
    % event({"PID", "Child PID is " ++ ChildPID}, S),
    {noreply, S#state{child_pid = ChildPID}};

handle_event({event, {<<"OUT">>, _Time, _Tags, Results}}, S) ->
    % event({"OUT", "Results at " ++ Results}, S),
    {noreply, S#state{results = Results}};

% rate limited event
handle_event({event, {Type, _Time, _Tags, Payload}}, S) ->
    Now = now(),
    EventGap = timer:now_diff(Now, S#state.last_event),
    if
        EventGap > ?RATE_WINDOW ->
            event({Type, Payload}, S),
            {noreply, S#state{last_event = Now, event_counter = 1}};
        S#state.event_counter > ?RATE_LIMIT ->
            error("Event rate limit exceeded. Too many msg() calls?", S);
        true ->
            event({Type, Payload}, S),
            {noreply, S#state{event_counter = S#state.event_counter + 1}}
    end;

handle_event({errline, _Line}, #state{errlines = {_Q, overflow, _Max}} = S) ->
    Garbage = message_buffer:to_string(S#state.errlines),
    error("Worker failed (too much garbage on stderr):\n" ++ Garbage, S);

handle_event({errline, Line}, S) ->
    {noreply, S#state{errlines =
        message_buffer:append(Line, S#state.errlines)}};

handle_event({malformed_event, Reason}, S) ->
    error(Reason, S);

handle_event(_EventState, S) ->
    {noreply, S}.

handle_info({_Port, {data, Data}}, #state{eventstream = EventStream} = S) ->
    EventStream1 = event_stream:feed(Data, EventStream),
    {next_stream, {_NextState, EventState}} = EventStream1,
    handle_event(EventState, S#state{eventstream = EventStream1,
                     linecount   = S#state.linecount + 1});

handle_info({_, {exit_status, _Status}}, #state{linecount = 0} = S) ->
    Reason = "Worker didn't start.\nSpawn command was: " ++ spawn_cmd(S) ++
        "Last words:\n" ++ message_buffer:to_string(S#state.errlines),
    error(recoverable, Reason, S);

handle_info({_, {exit_status, _Status}}, S) ->
    Reason =  "Worker failed. Last words:\n" ++ message_buffer:to_string(S#state.errlines),
    error(Reason, S);

handle_info({_, closed}, S) ->
    error(S);

handle_info(timeout, #state{linecount = 0} = S) ->
    error(recoverable, "Worker didn't start in 30 seconds", S);

handle_info({'DOWN', _, _, _, _}, S) ->
    error(S).

terminate(_Reason, State) ->
    % Possible bug: If we end up here before knowing child_pid, the
    % child may stay running. However, it may die by itself due to
    % SIGPIPE anyway.

    if State#state.child_pid =/= none ->
        % Kill child processes of the worker process
        os:cmd("pkill -9 -P " ++ State#state.child_pid),
        % Kill the worker process
        os:cmd("kill -9 " ++ State#state.child_pid);
    true -> ok
    end.

code_change(_OldVsn, State, _Extra) -> {ok, State}.
