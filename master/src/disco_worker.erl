-module(disco_worker).
-behaviour(gen_server).

-export([start_link/1, start_link_remote/4, remote_worker/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
    terminate/2, code_change/3, slave_name/1]).

-include("task.hrl").
-record(state, {id, master, job_url, port, task,
        start_time,
        eventserver,
        eventstream,
        child_pid, node,
        linecount, errlines,
        results,
        debug,
        last_event, event_counter,
        oob, oob_counter}).

-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 25).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).

-define(SLAVE_ARGS, "+K true").
-define(CMD, "nice -n 19 $DISCO_WORKER '~s' '~s' '~s' '~s' '~w' ~s").
-define(PORT_OPT, [{line, 100000}, binary, exit_status,
           use_stdio, stderr_to_stdout,
           {env, [{"LD_LIBRARY_PATH", "lib"}, {"LC_ALL", "C"}]}]).

slave_env() ->
    lists:flatten([?SLAVE_ARGS,
           io_lib:format(" -pa ~s/ebin", [disco:get_setting("DISCO_MASTER_HOME")]),
           [case disco:get_setting(Setting) of
                ""  -> "";
                Val -> io_lib:format(" -env ~s '~s'", [Setting, Val])
            end
            || Setting <- ["DISCO_FLAGS",
                   "DISCO_PORT",
                   "DISCO_PROXY",
                   "DISCO_ROOT",
                   "DISCO_WORKER",
                   "PYTHONPATH"]]]).

slave_name(Node) ->
    SName = lists:flatten([disco:get_setting("DISCO_NAME"), "_slave"]),
    list_to_atom(SName ++ "@" ++ Node).

start_link_remote(Master, Eventserver, Node, Task) ->
    NodeAtom = slave_name(Node),
    JobName = Task#task.jobname,

    case net_adm:ping(NodeAtom) of
        pong -> ok;
        pang ->
            slave_master ! {start, self(), Node, slave_env()},
            receive
                slave_started -> ok;
                {slave_failed, X} ->
                    exit({worker_dies,
                          {"Node failure: ~p", [X]}});
                X ->
                    exit({worker_dies,
                          {"Unknown node failure: ~p", [X]}})
            after 60000 ->
                exit({worker_dies, {"Node timeout", []}})
            end
    end,
    process_flag(trap_exit, true),

    {ok, JobUrl_} = application:get_env(disco_url),
    JobUrl = JobUrl_ ++ disco_server:jobhome(JobName),

    Debug = disco:get_setting("DISCO_DEBUG") =/= "off",
    spawn_link(NodeAtom, disco_worker, remote_worker,
           [[self(), Eventserver, Master, JobUrl, Task, Node, Debug]]),

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

remote_worker(Args) ->
    process_flag(trap_exit, true),
    start_link(Args),
    wait_for_exit().

wait_for_exit() ->
    receive
        {'EXIT', _, Reason} ->
            exit(Reason)
    end.

start_link([Parent|_] = Args) ->
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
    end.

init([Id, EventServer, Master, JobUrl, Task, Node, Debug]) ->
    process_flag(trap_exit, true),
    erlang:monitor(process, Task#task.from),
    {ok, #state{id = Id,
            master = Master,
            job_url = JobUrl,
            task = Task,
            node = Node,
            child_pid = none,
            eventserver = EventServer,
            eventstream = event_stream:new(),
            linecount = 0,
            start_time = now(),
            last_event = now(),
            event_counter = 0,
            oob = [],
            oob_counter = 0,
            errlines = message_buffer:new(?ERRLINES_MAX),
            debug = Debug,
            results = []}}.

spawn_cmd(#state{task = T, node = Node, job_url = JobUrl}) ->
    Args = [T#task.mode, T#task.jobname, Node, JobUrl, T#task.taskid, T#task.chosen_input],
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
    Port = open_port({spawn, Cmd}, ?PORT_OPT),
    {reply, ok, S#state{port = Port}, 30000}.

handle_cast(kill_worker, S) ->
    error(S).

handle_event({event, {<<"DAT">>, _Time, _Tags, Message}}, S) ->
    error(recoverable, Message, S);

handle_event({event, {<<"END">>, _Time, _Tags, _Message}}, S) ->
    event({"END", "Task finished in " ++ disco_server:format_time(S#state.start_time)}, S),
    {stop, worker_exit(S, {job_ok, {S#state.oob, S#state.results}}), S};

handle_event({event, {<<"ERR">>, _Time, _Tags, Message}}, S) ->
    error(Message, S);

handle_event({event, {<<"PID">>, _Time, _Tags, ChildPID}}, S) ->
    % event({"PID", "Child PID is " ++ ChildPID}, S),
    {noreply, S#state{child_pid = ChildPID}};

handle_event({event, {<<"OOB">>, _Time, _Tags, {_Key, _Path}}}, S)
        when S#state.oob_counter >= ?OOB_MAX ->
    Reason = "OOB message limit exceeded. Too many put() calls.",
    error(Reason, S);

handle_event({event, {<<"OOB">>, _Time, _Tags, {Key, Path}}}, S) ->
    event({"OOB", "OOB put " ++ Key ++ " at " ++ Path}, S),
    {noreply, S#state{oob = [{Key, Path}|S#state.oob],
              oob_counter = S#state.oob_counter + 1}};

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
    Reason =  "Worker didn't start:\n" ++ message_buffer:to_string(S#state.errlines),
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
