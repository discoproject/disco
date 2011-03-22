-module(disco_worker).
-behaviour(gen_server).

-export([start_link_remote/3, start_link/1]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("disco.hrl").

-record(state, {master :: node(),
                task :: task(),
                port :: port(),
                child_pid :: 'none' | string(),
                last_event :: timer:timestamp(),
                start_time :: timer:timestamp(),
                linecount :: non_neg_integer(),
                errlines :: message_buffer:message_buffer(),
                event_counter :: non_neg_integer(),
                event_stream :: event_stream:event_stream(),
                persisted_outputs :: [string()],
                output_filename :: 'none' | string(),
                output_file :: 'none' | file:io_device()}).

-define(RATE_WINDOW, 100000). % 100ms
-define(RATE_LIMIT, 25).
-define(ERRLINES_MAX, 100).
-define(OOB_MAX, 1000).

start_link_remote(Host, NodeMon, Task) ->
    Node = disco:slave_node(Host),
    wait_until_node_ready(NodeMon),
    spawn_link(Node, disco_worker, start_link, [{self(), node(), Task}]),
    process_flag(trap_exit, true),
    receive
        ok -> ok;
        {'EXIT', _, Reason} ->
            exit({error, Reason});
        _ ->
            exit({error, "Internal server error: invalid_reply"})
    after 60000 ->
            exit({error, "Worker did not start in 60s"})
    end,
    wait_for_exit().

wait_until_node_ready(NodeMon) ->
    NodeMon ! {is_ready, self()},
    receive
        node_ready -> ok
    after 30000 ->
        exit({error, "Node unavailable"})
    end.

wait_for_exit() ->
    receive
        {'EXIT', _, Reason} ->
            exit(Reason)
    end.

start_link({Parent, Master, Task}) ->
    process_flag(trap_exit, true),
    _Worker = case catch gen_server:start_link(disco_worker, {Master, Task}, []) of
                {ok, Server} ->
                     Server;
                 Reason ->
                     Msg = io_lib:fwrite("Worker initialization failed: ~p",
                                         [Reason]),
                     exit({error, Msg})
             end,
    Parent ! ok,
    % NB: start_worker call is known to timeout if the node is really
    % busy - it should not be a fatal problem
    %case catch gen_server:call(Worker, start_worker, 30000) of
    %    ok ->
    %        Parent ! ok;
    %    Reason1 ->
    %        exit({worker_dies, {"Worker startup failed: ~p",
    %            [Reason1]}})
    %end,
    wait_for_exit().

init({Master, Task}) ->
    erlang:monitor(process, Task#task.from),
    WorkerPid = self(),
    spawn(fun () ->
                  WorkerPid ! {work, make_jobhome(Task#task.jobname, Master)}
          end),
    {ok,
     #state{master = Master,
            task = Task,
            port = none,
            child_pid = none,
            last_event = now(),
            start_time = now(),
            linecount = 0,
            errlines = message_buffer:new(?ERRLINES_MAX),
            event_counter = 0,
            event_stream = event_stream:new(),
            persisted_outputs = [],
            output_filename = none,
            output_file = none},
     60000}.

worker_send(MsgName, Payload, #state{port = Port}) ->
    Msg = list_to_binary(MsgName),
    Data = list_to_binary(mochijson2:encode(Payload)),
    Length = list_to_binary(integer_to_list(size(Data))),
    port_command(Port, <<Msg/binary, " ", Length/binary, "\n", Data/binary, "\n">>).

event(Event, #state{master = Master, task = Task}) ->
    event_server:task_event(Task, Event, {}, disco:host(node()), {event_server, Master}).

handle_event({event, {<<"DAT">>, _Time, _Tags, Message}}, State) ->
    ok = close_output(State),
    {stop, {shutdown, {error, Message}}, State};

handle_event({event, {<<"END">>, _Time, _Tags, _Message}}, State) ->
    ok = close_output(State),
    Message = "Task finished in " ++ disco:format_time_since(State#state.start_time),
    event({<<"DONE">>, Message}, State),
    {stop, {shutdown, {done, results(State)}}, State};

handle_event({event, {<<"ERR">>, _Time, _Tags, Message}}, State) ->
    ok = close_output(State),
    {stop, {shutdown, {fatal, Message}}, State};

handle_event({event, {<<"JOB">>, _Time, _Tags, _Message}},
             #state{task = Task} = State) ->
    event({<<"JOB">>, "Job file requested"}, State),
    JobHome = jobhome(Task#task.jobname),
    worker_send("JOB", list_to_binary(jobpack:jobfile(JobHome)), State),
    {noreply, State};

handle_event({event, {<<"VSN">>, _Time, _Tags, ChildVSN}}, State) ->
    event({"VSN", "Child Version is " ++ binary_to_list(ChildVSN)}, State),
    worker_send("OK", <<"ok">>, State),
    {noreply, State};

handle_event({event, {<<"PID">>, _Time, _Tags, ChildPID}}, State) ->
    event({"PID", "Child PID is " ++ binary_to_list(ChildPID)}, State),
    worker_send("OK", <<"ok">>, State),
    {noreply, State#state{child_pid = binary_to_list(ChildPID)}};

handle_event({event, {<<"SET">>, _Time, _Tags, _Message}}, State) ->
    event({<<"SET">>, "Settings requested"}, State),
    Port = list_to_integer(disco:get_setting("DISCO_PORT")),
    PutPort = list_to_integer(disco:get_setting("DDFS_PUT_PORT")),
    Settings = {struct, [{<<"port">>, Port},
                         {<<"put_port">>, PutPort}]},
    worker_send("SET", Settings, State),
    {noreply, State};

handle_event({event, {<<"OUT">>, _Time, _Tags, Results}}, State) ->
    S = add_output(Results, State),
    worker_send("OK", <<"ok">>, S),
    {noreply, S};

handle_event({event, {<<"STA">>, _Time, _Tags, Message}}, State) ->
    event({<<"STA">>, Message}, State),
    worker_send("OK", <<"ok">>, State),
    {noreply, State};

handle_event({event, {<<"TSK">>, _Time, _Tags, _Message}},
             #state{task = Task} = State) ->
    event({<<"TSK">>, "Task info requested"}, State),
    TaskInfo = {struct, [{<<"taskid">>, Task#task.taskid},
                         {<<"master">>, list_to_binary(disco:get_setting("DISCO_MASTER"))},
                         {<<"mode">>, list_to_binary(Task#task.mode)},
                         {<<"jobname">>, list_to_binary(Task#task.jobname)},
                         {<<"host">>, list_to_binary(disco:host(node()))}]},
    worker_send("TSK", TaskInfo, State),
    {noreply, State};

handle_event({event, {<<"INP">>, _Time, _Tags, <<>>}}, #state{task = Task} = State) ->
    worker_send("INP",
                [<<"done">>, [[Id, Status, Urls] || {Id, Status, Urls} <- input(Task)]],
                State),
    {noreply, State};
handle_event({event, {<<"INP">>, _Time, _Tags, Id}}, #state{task = Task} = State) ->
    case lists:keyfind(Id, 1, input(Task)) of
        {Id, Status, Urls} ->
            worker_send("INP", [Status, Urls], State);
        false ->
            worker_send("ERROR", [<<"No such input">>, Id], State)
    end,
    {noreply, State};

% rate limited event
handle_event({event, {Type, _Time, _Tags, Payload}}, State) ->
    Now = now(),
    EventGap = timer:now_diff(Now, State#state.last_event),
    if EventGap > ?RATE_WINDOW ->
            event({Type, Payload}, State),
            worker_send("OK", <<"ok">>, State),
            {noreply, State#state{last_event = Now, event_counter = 1}};
       State#state.event_counter > ?RATE_LIMIT ->
            {stop, {shutdown, {fatal, "Event rate limit exceeded. Too many msg() calls?"}}, State};
       true ->
            event({Type, Payload}, State),
            worker_send("OK", <<"ok">>, State),
            {noreply, State#state{event_counter = State#state.event_counter + 1}}
    end;

handle_event({errline, _Line}, #state{errlines = {_Q, overflow, _Max}} = State) ->
    Garbage = message_buffer:to_string(State#state.errlines),
    {stop, {shutdown, {fatal, "Worker failed:\n" ++ Garbage}}, State};
handle_event({errline, Line}, State) ->
    {noreply, State#state{errlines = message_buffer:append(Line, State#state.errlines)}};

handle_event({malformed_event, Reason}, State) ->
    {stop, {shutdown, {fatal, Reason}}, State};
handle_event(_EventState, State) ->
    {noreply, State}.

handle_info(timeout, State) ->
    {stop, {shutdown, {fatal, "Worker timed out"}}, State};

handle_info({_Port, {data, Data}}, #state{event_stream = EventStream} = State) ->
    EventStream1 = event_stream:feed(Data, EventStream),
    {next_stream, {_NextState, EventState}} = EventStream1,
    handle_event(EventState, State#state{event_stream = EventStream1,
                                         linecount = State#state.linecount + 1});
handle_info({_Port, {exit_status, _Status}}, State) ->
    Reason =  "Worker died. Last words:\n" ++ message_buffer:to_string(State#state.errlines),
    {stop, {shutdown, {fatal, Reason}}, State};

handle_info({'DOWN', _, _, _, Info}, State) ->
    {stop, {shutdown, {fatal, Info}}, State};

handle_info({work, JobHome}, #state{task = Task, port = none} = State) ->
    Worker = filename:join(JobHome, binary_to_list(Task#task.worker)),
    file:change_mode(Worker, 8#755),
    Command = "nice -n 19 " ++ Worker,
    JobEnvs = jobpack:jobenvs(jobpack:read(JobHome)),
    Options = [{cd, JobHome},
               {line, 100000},
               binary,
               exit_status,
               use_stdio,
               stderr_to_stdout,
               {env, dict:to_list(JobEnvs)}],
    {noreply, State#state{port = open_port({spawn, Command}, Options)}}.

handle_call(kill_worker, _From, State) ->
    {stop, {shutdown, {fatal, "Worker killed"}}, State}.

handle_cast(kill_worker, State) ->
    {stop, {shutdown, {fatal, "Worker killed"}}, State}.

terminate(_Reason, #state{child_pid = Pid}) when Pid =/= none ->
    % Kill child processes of the worker process
    os:cmd("pkill -9 -P " ++ Pid),
    % Kill the worker process
    os:cmd("kill -9 " ++ Pid);
terminate(_Reason, State) ->
    event({<<"WARNING">>, "PID unknown: worker could not be killed"}, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

jobhome(JobName) ->
    Home = filename:join(disco:get_setting("DISCO_DATA"), disco:host(node())),
    disco:jobhome(JobName, Home).

make_jobhome(JobName, Master) ->
    JobHome = jobhome(JobName),
    case jobpack:extracted(JobHome) of
        true ->
            JobHome;
        false ->
            make_jobhome(JobName, JobHome, Master)
    end.
make_jobhome(JobName, JobHome, Master) ->
    JobAtom = list_to_atom(disco:hexhash(JobName)),
    case catch register(JobAtom, self()) of
        true ->
            disco:make_dir(JobHome),
            JobPack = gen_server:call({disco_server, Master},
                                      {jobpack, JobName}),
            jobpack:save(JobPack, JobHome),
            jobpack:extract(JobPack, JobHome),
            JobHome;
        _Else ->
            wait_for_jobhome(JobAtom, JobName, Master)
    end.

wait_for_jobhome(JobAtom, JobName, Master) ->
    case whereis(JobAtom) of
        undefined ->
            make_jobhome(JobName, Master);
        JobProc ->
            process_flag(trap_exit, true),
            link(JobProc),
            receive
                _Any ->
                    make_jobhome(JobName, Master)
            end
    end.

input(Task) ->
    case Task#task.chosen_input of
        Binary when is_binary(Binary) ->
            [{1, <<"ok">>, [Binary]}];
        List when is_list(List) ->
            [{I, <<"ok">>, lists:flatten([Url])} || {I, Url} <- disco:enum(List)]
    end.

results_filename(Task) ->
    TimeStamp = timer:now_diff(now(), {0,0,0}),
    FileName = io_lib:format("~s-~B-~B.results", [Task#task.mode,
                                                  Task#task.taskid,
                                                  TimeStamp]),
    filename:join(".disco", FileName).

url_path(Task, Host, LocalFile) ->
    LocationPrefix = disco:joburl(Host, Task#task.jobname),
    filename:join(LocationPrefix, LocalFile).

local_results(Task, FileName) ->
    Host = disco:host(node()),
    Output = io_lib:format("dir://~s/~s",
                           [Host, url_path(Task, Host, FileName)]),
    list_to_binary(Output).

results(#state{output_filename = none, persisted_outputs = Outputs}) ->
    {none, Outputs};
results(#state{task = Task,
               output_filename = FileName,
               persisted_outputs = Outputs}) ->
    {local_results(Task, FileName), Outputs}.

format_output_line(S, [LocalFile, Type]) ->
    format_output_line(S, [LocalFile, Type, <<"0">>]);
format_output_line(#state{task = Task}, [LocalFile, Type, Label]) ->
    Host = disco:host(node()),
    io_lib:format("~s ~s://~s/~s\n",
                  [Label, Type, Host, url_path(Task,
                                               Host,
                                               binary_to_list(LocalFile))]).

-spec add_output(list(), #state{}) -> #state{}.
add_output([Tag, <<"tag">>], S) ->
    Result = list_to_binary(io_lib:format("tag://~s", [Tag])),
    Outputs = [Result | S#state.persisted_outputs],
    S#state{persisted_outputs = Outputs};

add_output(RL, #state{task = Task, output_file = none} = S) ->
    ResultsFileName = results_filename(Task),
    Path = filename:join(jobhome(Task#task.jobname), ResultsFileName),
    ok = filelib:ensure_dir(Path),
    {ok, ResultsFile} = prim_file:open(Path, [write, raw]),
    add_output(RL, S#state{output_filename = ResultsFileName,
                           output_file = ResultsFile});

add_output(RL, #state{output_file = RF} = S) ->
    prim_file:write(RF, format_output_line(S, RL)),
    S.

-spec close_output(#state{}) -> 'ok'.
close_output(#state{output_file = none}) -> ok;
close_output(#state{output_file = File}) ->
    prim_file:close(File),
    ok.
