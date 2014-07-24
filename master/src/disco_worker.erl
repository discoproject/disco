-module(disco_worker).
-behaviour(gen_server).

-export([start_link_remote/3,
         start_link/3,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         jobhome/1,
         add_inputs/2,
         terminate_inputs/1,
         event/3]).

-include("common_types.hrl").
-include("gs_util.hrl").
-include("disco.hrl").
-include("pipeline.hrl").

-record(state, {master :: node(),
                task :: task(),
                port :: none | port(),
                worker_send :: none | pid(),
                error_output :: boolean(),
                buffer :: binary(),
                parser :: worker_protocol:state(),
                runtime :: worker_runtime:state(),
                throttle :: worker_throttle:state()}).
-type state() :: #state{}.
-type shutdown() :: {error | fatal | done, term()}.

-export_type([shutdown/0]).

-define(JOBHOME_TIMEOUT, 5 * 60 * 1000).
-define(PID_TIMEOUT, 30 * 1000).
-define(ERROR_TIMEOUT, 10 * 1000).
-define(MESSAGE_TIMEOUT, 30 * 1000).
-define(MAX_ERROR_BUFFER_SIZE, 100 * 1024).

-spec start_link_remote(host(), pid(), task()) -> no_return().
start_link_remote(Host, NodeMon, Task) ->
    Node = disco:slave_node(Host),
    wait_until_node_ready(NodeMon, Host),
    process_flag(trap_exit, true),
    {Master, Self} = {node(), self()},
    spawn_link(Node, disco_worker, start_link, [Self, Master, Task]),
    receive
        {'EXIT', _, Reason} -> exit({error, Reason});
        ok      -> ok;
        _       -> exit({error, "Internal server error: invalid_reply"})
    after 60000 -> exit({error, "Worker did not start in 60s at " ++ Host})
    end,
    wait_for_exit().

-spec wait_until_node_ready(pid(), host()) -> ok.
wait_until_node_ready(NodeMon, Host) ->
    NodeMon ! {is_ready, self()},
    receive node_ready -> ok
    after 30000 -> exit({error, lists:flatten(["Node ", Host, " unavailable"])})
    end.

-spec wait_for_exit() -> no_return().
wait_for_exit() ->
    receive {'EXIT', _, Reason} -> exit(Reason)
    end.

-spec start_link(pid(), node(), task()) -> no_return().
start_link(Parent, Master, Task) ->
    process_flag(trap_exit, true),
    {ok, Server} = gen_server:start_link(?MODULE, {Master, Task}, []),
    gen_server:cast(Server, start),
    Parent ! ok,
    wait_for_exit().

-spec init({node(), task()}) -> gs_init().
init({Master, {#task_spec{job_coord = JobCoord}, #task_run{}} = Task}) ->
    % Note! This worker is killed implicitly by killing its
    % job_coordinator, which should be noticed by the monitor
    % below.
    erlang:monitor(process, JobCoord),
    % introduce myself to the job_coordinator as the worker
    {#task_spec{taskid = TaskId}, _} = Task,
    job_coordinator:task_started(JobCoord, TaskId, self()),
    {ok, #state{master = Master,
                task = Task,
                port = none,
                worker_send = none,
                error_output = false,
                buffer = <<>>,
                parser = worker_protocol:init(),
                runtime = worker_runtime:init(Task, Master),
                throttle = worker_throttle:init()}}.

-spec add_inputs(pid(), [{input_id(), data_input()}]) -> ok.
add_inputs(Worker, Inputs) ->
    gen_server:cast(Worker, {input, Inputs}).

-spec terminate_inputs(pid()) -> ok.
terminate_inputs(Worker) ->
    gen_server:cast(Worker, {input, done}).

-spec handle_cast(start | work, state()) -> gs_noreply();
                 ({input, [{input_id(), data_input()}]}, state()) -> gs_noreply().
handle_cast({input, Inputs}, #state{runtime = Runtime} = S) ->
    {Reply, Runtime1} = worker_runtime:add_inputs(Inputs, Runtime),
    case Reply of
        none ->
            {noreply, S#state{runtime = Runtime1}};
        _ ->
            update(S#state{runtime = Runtime1})
    end;

handle_cast(start, #state{task = Task, master = Master} = State) ->
    {#task_spec{jobname = JobName}, #task_run{}} = Task,
    Fun = fun() -> make_jobhome(JobName, Master) end,
    try case lock_server:lock(JobName, Fun, ?JOBHOME_TIMEOUT) of
            ok ->
                gen_server:cast(self(), work),
                {noreply, State};
            {error, Reason} ->
                Msg = io_lib:format("Jobpack extraction failed: ~p", [Reason]),
                {stop, {shutdown, {error, Msg}}, State}
        end
    catch K:V ->
            E = io_lib:format("Jobpack extraction error: ~p:~p", [K,V]),
            {stop, {shutdown, {error, E}}, State}
    end;
handle_cast(work, #state{task = T, port = none} = State) ->
    {#task_spec{jobname = JobName, worker = W, jobenvs = JE}, #task_run{}} = T,
    JobHome = jobhome(JobName),
    Worker = filename:join(JobHome, binary_to_list(W)),
    Command = "nice -n 19 " ++ Worker,
    JobEnvs = [{S, false} || S <- disco:settings()] ++ JE,
    Options = [{cd, JobHome},
               stream,
               binary,
               exit_status,
               use_stdio,
               stderr_to_stdout,
               {env, JobEnvs}],
    Port = open_port({spawn, Command}, Options),
    SendPid = spawn_link(fun() -> worker_send(T, Port) end),
    {noreply, State#state{port = Port, worker_send = SendPid}, ?PID_TIMEOUT}.


-type port_msg() :: timeout | {port(), {data, binary()}}
                  | {port(), {exit_status, non_neg_integer()}}
                  | {'DOWN', _, _, _, _}.
-spec handle_info(port_msg(), state()) -> gs_noreply() | gs_stop(shutdown()).
handle_info({_Port, {data, Data}},
            #state{error_output = true, buffer = Buffer} = State)
            when size(Buffer) < ?MAX_ERROR_BUFFER_SIZE ->
    Buffer1 = <<Buffer/binary, Data/binary>>,
    {noreply, State#state{buffer = Buffer1}, ?ERROR_TIMEOUT};

handle_info({_Port, {data, _Data}}, #state{error_output = true} = State) ->
    exit_on_error(State);

handle_info({_Port, {data, Data}}, #state{buffer = Buffer} = S) ->
    update(S#state{buffer = <<Buffer/binary, Data/binary>>});

handle_info(timeout, #state{error_output = false, runtime = Runtime} = S) ->
    case worker_runtime:get_pid(Runtime) of
        none -> warning("Worker did not send its PID in 30 seconds", S);
        _    -> warning("Worker stuck in the middle of a message", S)
    end,
    exit_on_error(S);

handle_info(timeout, S) ->
    warning("Worker did not exit properly after error", S),
    exit_on_error(S);

handle_info({_Port, {exit_status, Code}}, S) ->
    warning(["Worker crashed! (exit code: ", integer_to_list(Code), ")"], S),
    exit_on_error(S);

handle_info({'DOWN', _, _, _, Info}, State) ->
    {stop, {shutdown, {fatal, Info}}, State}.

-spec handle_call(term(), from(), state()) -> gs_noreply().
handle_call(_Req, _From, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, #state{port = none} = _State) ->
    ok;
terminate(_Reason, #state{runtime = Runtime} = S) ->
    case worker_runtime:get_pid(Runtime) of
        none ->
            warning("PID unknown: worker could not be killed", S);
        Pid ->
            PidStr = integer_to_list(Pid),
            % Kill child processes of the worker process
            _ = os:cmd(["pkill -9 -P ", PidStr]),
            % Kill the worker process
            _ = os:cmd(["kill -9 ",  PidStr])
    end.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-spec proto_log(task(), to|from, nonempty_string(), [term()]) -> ok.
-ifdef(LOG_PROTOCOL).
proto_log(T, Dir, Format, Args) ->
    {#task_spec{jobname = J, stage = S, taskid = Tid}, _} = T,
    D = case Dir of to -> "->"; from -> "<-" end,
    Msg = disco:format(Format, Args),
    error_logger:info_msg("~p:~s:~p ~s ~p", [J, S, Tid, D, Msg]).
-else.
proto_log(_T, _Dir, _Format, _Args) -> ok.
-endif.

-spec update(state()) -> {'noreply', state()} | {'stop', shutdown(), state()}.
% Note that size(Buffer) =:= 0 is here to avoid preventing delayed sub
% binary optimization in worker_protocol:parse (since we use the
% binary in a byte_size guard), resulting in expensive copying of
% Buffer. See
% http://www.erlang.org/doc/efficiency_guide/binaryhandling.html
update(#state{buffer = Buffer} = S) when size(Buffer) =:= 0 ->
    {noreply, S};
update(#state{task = Task,
              buffer = B,
              parser = P,
              runtime = RT,
              worker_send = WS,
              throttle = T} = S) ->
    case worker_protocol:parse(B, P) of
        {ok, Request, Buffer, PState} ->
            proto_log(Task, from, "~p", [Request]),
            S1 = S#state{buffer = Buffer, parser = PState},
            try case worker_runtime:handle(Request, RT) of
                    {ok, noreply, RState} ->
                        % The following should be S (not S1) because disco did not
                        % consume the stuff the worker sent to it.
                        {noreply, S#state{runtime = RState}};
                    {ok, Reply, RState} ->
                        WS ! {Reply, 0},
                        update(S1#state{runtime = RState});
                    {ok, Reply, RState, rate_limit} ->
                        case worker_throttle:handle(T) of
                            {ok, Delay, TState} ->
                                WS ! {Reply, Delay},
                                update(S1#state{runtime = RState,
                                                throttle = TState});
                            {error, Msg} ->
                                S2 = S1#state{runtime = RState},
                                warning(Msg, S2),
                                exit_on_error(fatal, S2)
                        end;
                    {stop, Ret} ->
                        {stop, {shutdown, Ret}, S1};
                    {error, {Type, Msg}} ->
                        warning(Msg, S1),
                        exit_on_error(Type, S1);
                    {error, {Type, Msg}, RState} ->
                        S2 = S1#state{runtime = RState},
                        warning(Msg, S2),
                        exit_on_error(Type, S2)
                end
            catch K:V ->
                    warning(io_lib:format("~p:~p", [K, V]), S1),
                    exit_on_error(error, S1)
            end;
        {cont, Buffer, PState} ->
            {noreply, S#state{buffer = Buffer, parser = PState}, ?MESSAGE_TIMEOUT};
        {error, Type} ->
            warning(["Could not parse worker event: ", atom_to_list(Type)], S),
            handle_info({none, {data, <<>>}}, S#state{error_output = true})
    end.

-spec worker_send(task(), port()) -> no_return().
worker_send(Task, Port) ->
    receive
        {{MsgName, Payload}, Delay} ->
            timer:sleep(Delay),
            Type = list_to_binary(MsgName),
            Body = list_to_binary(mochijson2:encode(Payload)),
            Length = list_to_binary(integer_to_list(byte_size(Body))),
            Msg = <<Type/binary, " ", Length/binary, " ", Body/binary, "\n">>,
            port_command(Port, Msg),
            proto_log(Task, to, "~s ~p", [MsgName, Body]),
            worker_send(Task, Port)
    end.

-spec make_jobhome(jobname(), node()) -> ok.
make_jobhome(JobName, Master) ->
    JobHome = jobhome(JobName),
    case jobpack:extracted(JobHome) of
        true ->
            ok;
        false ->
            {ok, _} = disco:make_dir(JobHome),
            JobPack =
                case jobpack:exists(JobHome) of
                    true ->
                        jobpack:read(JobHome);
                    false ->
                        {ok, JobPackSrc} =
                            disco_server:get_worker_jobpack(Master, JobName),
                        {ok, _JobFile} = jobpack:copy(JobPackSrc, JobHome),
                        jobpack:read(JobHome)
                end,
            jobpack:extract(JobPack, JobHome)
    end.

-spec jobhome(jobname()) -> path().
jobhome(JobName) ->
    Home = filename:join(disco:get_setting("DISCO_DATA"), disco:host(node())),
    disco:jobhome(JobName, Home).

warning(Msg, #state{master = Master, task = Task}) ->
    event({<<"WARNING">>, iolist_to_binary(Msg)}, Task, Master).

-spec event(job_event:task_msg(), task(), node()) -> ok.
event(M, {#task_spec{jehandler = JEHandler, jobname = J, taskid = T, stage = S}, _}, _Master) ->
    Host = disco:host(node()),
    job_event:task_event(JEHandler, {J, S, T}, M, none, Host).

exit_on_error(S) ->
    exit_on_error(error, S).

exit_on_error(Type, #state{buffer = <<>>} = S) ->
    {stop, {shutdown, {Type, "Worker died without output"}}, S};
exit_on_error(Type, #state{buffer = Buffer} = S)
  when byte_size(Buffer) > ?MAX_ERROR_BUFFER_SIZE ->
    <<Buffer1:(?MAX_ERROR_BUFFER_SIZE - 3)/binary, _/binary>> = Buffer,
    exit_on_error(Type, S#state{buffer = <<Buffer1/binary, "...">>});
exit_on_error(Type, #state{buffer = Buffer} = S) ->
    Msg = iolist_to_binary(["Worker died. Last words:\n", Buffer]),
    {stop, {shutdown, {Type, Msg}}, S}.

