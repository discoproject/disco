-module(disco_worker).
-behaviour(gen_server).

-export([start_worker/2]).
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

start_worker(Master, Task) ->
    gen_server:start(?MODULE, {Master, Task}, [{timeout, 30000}]).

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
    Data = dencode:encode(Payload),
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
    Settings = dict:from_list([{<<"port">>, Port},
                               {<<"put_port">>, PutPort}]),
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
    TaskInfo = dict:from_list([{<<"taskid">>, Task#task.taskid},
                               {<<"mode">>, list_to_binary(Task#task.mode)},
                               {<<"jobname">>, list_to_binary(Task#task.jobname)},
                               {<<"host">>, list_to_binary(disco:host(node()))}]),
    worker_send("TSK", TaskInfo, State),
    {noreply, State};

handle_event({event, {<<"INP">>, _Time, _Tags, _Message}}, #state{task = Task} = State) ->
    % The response structure is:
    % (more|done, [{integer_id, "ok"|"busy"|"failed", ["url"+]}])
    % For now, we will always return a non-incremental response:
    % ('done', [{id, "failed", ["url"+ ]}])
    Inputs = case Task#task.chosen_input of
                 Binary when is_binary(Binary) ->
                     [[0, <<"failed">>, [Binary]]];
                 List when is_list(List) ->
                     Enum = lists:seq(0, length(List)-1),
                     [[I, <<"failed">>, [Url]] || {I,Url} <- lists:zip(Enum, List)]
             end,
    worker_send("INP", [<<"done">>, Inputs], State),
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
handle_info({work, JobHome}, #state{task = Task, port = none} = State) ->
    Worker = filename:join(JobHome, binary_to_list(Task#task.worker)),
    file:change_mode(Worker, 8#755),
    Command = "nice -n 19 " ++ Worker,
    Options = [{cd, JobHome},
               {line, 100000},
               binary,
               exit_status,
               use_stdio,
               stderr_to_stdout,
               {env,
                [{"LD_LIBRARY_PATH", "lib"},
                 {"LC_ALL", "C"}] ++
                [{Setting, disco:get_setting(Setting)}
                 || Setting <- disco:settings()]}],
    {noreply, State#state{port = open_port({spawn, Command}, Options)}};

handle_info({_Port, {data, Data}}, #state{event_stream = EventStream} = State) ->
    EventStream1 = event_stream:feed(Data, EventStream),
    {next_stream, {_NextState, EventState}} = EventStream1,
    handle_event(EventState, State#state{event_stream = EventStream1,
                                         linecount = State#state.linecount + 1});
handle_info({_Port, {exit_status, _Status}}, State) ->
    Reason =  "Worker died. Last words:\n" ++ message_buffer:to_string(State#state.errlines),
    {stop, {shutdown, {fatal, Reason}}, State};

handle_info({'DOWN', _, _, _, Info}, State) ->
    {stop, {shutdown, {fatal, Info}}, State}.

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
    event({<<"WARN">>, "PID unknown: worker could not be killed"}, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

jobhome(JobName) ->
    disco:jobhome(JobName, disco_node:home()).

make_jobhome(JobName, Master) ->
    JobHome = jobhome(JobName),
    case jobpack:extracted(JobHome) of
        true -> JobHome;
        false -> make_jobhome(JobName, JobHome, Master)
    end.
make_jobhome(JobName, JobHome, Master) ->
    JobAtom = list_to_atom(disco:hexhash(JobName)),
    case catch register(JobAtom, self()) of
        true ->
            disco:make_dir(JobHome),
            JobPack = disco_server:get_worker_jobpack(Master, JobName),
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
                {'EXIT', noproc} ->
                    make_jobhome(JobName, Master);
                {'EXIT', JobProc, normal} ->
                    make_jobhome(JobName, Master)
            end
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
results(#state{output_filename = FileName, task = Task,
               persisted_outputs = Outputs}) ->
    {local_results(Task, FileName), Outputs}.

format_output_line(S, [LocalFile, Type]) ->
    format_output_line(S, [LocalFile, Type, <<"0">>]);
format_output_line(#state{task = Task},
                   [BLocalFile, Type, Label]) ->
    Host = disco:host(node()),
    LocalFile = binary_to_list(BLocalFile),
    io_lib:format("~s ~s://~s/~s\n",
                  [Label, Type, Host, url_path(Task, Host, LocalFile)]).

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
