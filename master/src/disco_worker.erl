-module(disco_worker).
-behaviour(gen_server).

-export([start_link/1, start_link_remote/4, remote_worker/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3, slave_name/1]).

-include("task.hrl").
-record(state, {id, master, master_url, eventserv, port, task,
                child_pid, node, linecount, errlines, results,
                debug, last_msg, msg_counter, oob, oob_counter,
                start_time}).

-define(MAX_MSG_LENGTH, 8192).
-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 10).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).
-define(OOB_KEY_MAX, 256).

-define(SLAVE_ARGS, "+K true").
-define(CMD, "nice -n 19 $DISCO_WORKER '~s' '~s' '~s' '~s' '~w' ~s").
-define(PORT_OPT, [{line, 100000}, binary, exit_status,
                   use_stdio, stderr_to_stdout, 
                   {env, [{"LD_LIBRARY_PATH", "lib"}, {"LC_ALL", "C"}]}]).

get_env(Var) ->
        get_env(Var, lists:flatten(io_lib:format(" -env ~s '~~s'", [Var]))).

get_env(Var, Fmt) ->
        case os:getenv(Var) of
                false -> "";
                "" -> "";
                Val -> io_lib:format(Fmt, [Val])
        end.

slave_env() ->
    lists:flatten([?SLAVE_ARGS, 
                   get_env("DISCO_MASTER_HOME", " -pa ~s/ebin"),
                   [get_env(X) || X <- ["DISCO_MASTER_PORT",
                                        "DISCO_ROOT",
                                        "DISCO_PORT",
                                        "DISCO_WORKER",
                                        "DISCO_FLAGS",
                                        "PYTHONPATH"]]]).

slave_name(Node) ->
        {ok, Name} = application:get_env(disco_name), %get_env("DISCO_NAME"),
        SName = lists:flatten([Name, "_slave"]),
        list_to_atom(SName ++ "@" ++ Node).

start_link_remote(Master, EventServ, Node, Task) ->
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

        {ok, MasterUrl0} = application:get_env(disco_url),
        MasterUrl = MasterUrl0 ++ disco_server:jobhome(JobName),

        Debug = os:getenv("DISCO_DEBUG") =/= false,
        spawn_link(NodeAtom, disco_worker, remote_worker,
                 [[self(), EventServ, Master, MasterUrl, Task, Node, Debug]]),
        
        receive
                ok -> ok;
                {'EXIT', _, Reason} -> exit(Reason);
                _ -> exit({error, invalid_reply})
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
                {'EXIT', _, Reason} -> exit(Reason)
        end.

start_link([Parent|_] = Args) ->
        Worker = case catch gen_server:start_link(disco_worker, Args, []) of
                {ok, Server} -> Server;
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

init([Id, EventServ, Master, MasterUrl, Task, Node, Debug]) ->
        process_flag(trap_exit, true),
        erlang:monitor(process, Task#task.from),
        {ok, #state{id = Id, 
                    master = Master,
                    master_url = MasterUrl,
                    task = Task,
                    node = Node,
                    child_pid = none, 
                    eventserv = EventServ,
                    linecount = 0,
                    start_time = now(),
                    last_msg = now(),
                    msg_counter = 0,
                    oob = [],
                    oob_counter = 0,
                    errlines = [],
                    debug = Debug,
                    results = []}}.

handle_call(start_worker, _From, S) ->
        Cmd = spawn_cmd(S),
        if S#state.debug ->
                error_logger:info_report(["Spawn cmd: ", Cmd]);
        true -> ok end,
        Port = open_port({spawn, Cmd}, ?PORT_OPT),
        {reply, ok, S#state{port = Port}, 30000}.

spawn_cmd(#state{task = T, node = Node, master_url = Url}) ->
        lists:flatten(io_lib:fwrite(?CMD,
                [T#task.mode, T#task.jobname, Node, Url, 
                        T#task.taskid, T#task.chosen_input])).

strip_timestamp(Msg) when is_binary(Msg) ->
        strip_timestamp(binary_to_list(Msg));
strip_timestamp(Msg) ->
        P = string:chr(Msg, $]),
        if P == 0 ->
                Msg;
        true ->
                string:substr(Msg, P + 2)
        end.

event(#state{task = T, eventserv = EvServ, node = Node}, "WARN", Msg) ->
        event_server:event(EvServ, Node, T#task.jobname,
                "~s [~s:~B] ~s", ["WARN", T#task.mode, T#task.taskid, Msg],
                        {task_failed, T#task.mode});

event(#state{task = T, eventserv = EvServ, node = Node}, Type, Msg) ->
        event_server:event(EvServ, Node, T#task.jobname,
                "~s [~s:~B] ~s", [Type, T#task.mode, T#task.taskid, Msg], []).

worker_exit(#state{id = Id, master = Master}, Msg) ->
        gen_server:cast(Master, {exit_worker, Id, Msg}),
        normal.

errlines(#state{errlines = L}) -> lists:flatten(lists:reverse(L)).

handle_info({_, {data, {eol, <<"**<PID>", Line/binary>>}}}, S) ->
        {noreply, S#state{child_pid = binary_to_list(Line)}}; 

handle_info({_, {data, {eol, <<"**<MSG>", Line0/binary>>}}}, S) ->
        if size(Line0) > ?MAX_MSG_LENGTH ->
                <<Line:?MAX_MSG_LENGTH/binary, _/binary>> = Line0;
        true ->
                Line = Line0
        end,
        event(S, "", strip_timestamp(Line)),
        T = now(),
        D = timer:now_diff(T, S#state.last_msg),
        S1 = S#state{last_msg = T, linecount = S#state.linecount + 1},
        if D > ?RATE_WINDOW ->
                {noreply, S1#state{msg_counter = 1}};
        S1#state.msg_counter > ?RATE_LIMIT ->
                Err = "Message rate limit exceeded. Too many msg() calls.",
                event(S, "ERROR", Err),
                {stop, worker_exit(S1, {job_error, Err}), S1};
        true ->
                {noreply, S1#state{msg_counter = S1#state.msg_counter + 1}}
        end;

handle_info({_, {data, {eol, <<"**<ERR>", Line/binary>>}}}, S) ->
        M = strip_timestamp(Line),
        event(S, "ERROR", M),
        {stop, worker_exit(S, {job_error, M}), S};

handle_info({_, {data, {eol, <<"**<DAT>", Line/binary>>}}}, S) ->
        M = strip_timestamp(Line),
        event(S, "WARN", M ++ [10] ++ errlines(S)),
        {stop, worker_exit(S, {data_error, M}), S};

handle_info({_, {data, {eol, <<"**<OUT>", Line/binary>>}}}, S) ->
        {noreply, S#state{results = strip_timestamp(Line)}};

handle_info({_, {data, {eol, <<"**<END>", Line/binary>>}}}, S) ->
        event(S, "", strip_timestamp(Line)),
        event(S, "", "Task finished in " ++ 
                disco_server:format_time(S#state.start_time)),
        {stop, worker_exit(S, {job_ok, {S#state.oob, S#state.results}}), S};

handle_info({_, {data, {eol, <<"**<OOB>", Line/binary>>}}}, S) ->
        [Key|Path] = string:tokens(binary_to_list(Line), " "),

        S1 = S#state{oob = [{Key, Path}|S#state.oob],
                     oob_counter = S#state.oob_counter + 1},

        if length(Key) > ?OOB_KEY_MAX ->
                Err = "OOB key too long: Max 256 characters",
                event(S, "ERROR", Err), 
                {stop, worker_exit(S1, {job_error, Err}), S1};
        S#state.oob_counter > ?OOB_MAX ->
                Err = "OOB message limit exceeded. Too many put() calls.",
                event(S, "ERROR", Err), 
                {stop, worker_exit(S1, {job_error, Err}), S1};
        true ->
                {noreply, S1}
        end;

handle_info({_, {data, {eol, _}}}, S) when
                length(S#state.errlines) > ?ERRLINES_MAX ->
        Err = "Worker failed (too much output on stdout):\n" ++
                        errlines(S),
        event(S, "ERROR", Err),
        {stop, worker_exit(S, {job_error, Err}), S};

handle_info({_, {data, {eol, Line}}}, S) ->
        {noreply, S#state{errlines =
                [[binary_to_list(Line), 10]|S#state.errlines]}};

handle_info({_, {data, {noeol, Line}}}, S) ->
        {noreply, S#state{errlines =
                [[binary_to_list(Line), "...", 10]|S#state.errlines]}};

handle_info({_, {exit_status, _Status}}, #state{linecount = 0} = S) ->
        M =  "Worker didn't start:\n" ++ errlines(S),
        event(S, "WARN", M),
        {stop, worker_exit(S, {data_error, M}), S};

handle_info({_, {exit_status, _Status}}, S) ->
        M =  "Worker failed. Last words:\n" ++ errlines(S),
        event(S, "ERROR", M),
        {stop, worker_exit(S, {job_error, M}), S};
        
handle_info({_, closed}, S) ->
        {stop, worker_exit(S, {job_error, "Worker killed"}), S};

handle_info(timeout, #state{linecount = 0} = S) ->
        M = "Worker didn't start in 30 seconds",
        event(S, "WARN", M),
        {stop, worker_exit(S, {data_error, M}), S};

handle_info({'DOWN', _, _, _, _}, S) ->
        {stop, worker_exit(S, {job_error, "Worker killed"}), S}.

handle_cast(kill_worker, S) -> 
        {stop, worker_exit(S, {job_error, "Worker killed"}), S}.

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

