
-module(disco_worker).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-record(state, {port, from, jobname, partid, mode, 
                node, input, data, linecount, errlines, results}).

-define(CMD, "disco_worker.sh '~s' '~s' '~s' '~w' '~s'").
-define(PORT_OPT, [{line, 100000}, exit_status, use_stdio, stderr_to_stdout]).

start_link(Args) ->
        {ok, _} = gen_server:start_link(disco_worker, Args, []).

init([From, JobName, PartID, Mode, Node, Input, Data]) ->
        error_logger:info_report(["Init worker ", JobName]),
        ets:insert(active_workers, {self(), {From, JobName, Node, Mode, PartID}}),

        {ok, #state{from = From, jobname = JobName, partid = PartID, mode = Mode,
                    node = Node, input = Input, data = Data, 
                    linecount = 0, errlines = [], results = []}}.

spawn_cmd(#state{input = [Input|_]} = S) when is_list(Input) ->
        InputStr = lists:flatten([[X, 32] || X <- S#state.input]),
        spawn_cmd(S#state{input = InputStr});

spawn_cmd(#state{input = [Input|_]} = S) when is_binary(Input) ->
        InputStr = lists:flatten([[binary_to_list(X), 32] || X <- S#state.input]),
        spawn_cmd(S#state{input = InputStr});

spawn_cmd(#state{jobname = JobName, node = Node, partid = PartID,
                mode = Mode, input = Input}) ->
        lists:flatten(io_lib:fwrite(?CMD,
                [Mode, JobName, Node, PartID, Input])).

handle_call(start_worker, _From, State) ->
        Cmd = spawn_cmd(State),
        error_logger:info_report(["Spawn cmd: ", Cmd]),
        Port = open_port({spawn, spawn_cmd(State)}, ?PORT_OPT),
        port_command(Port, State#state.data),
        {reply, ok, State#state{port = Port}, 30000};

handle_call(kill_worker, _From, State) ->
        error_logger:info_report(["Kill worker"]),
        State#state.port ! {self(), close},
        {reply, ok, State}.

strip_timestamp(Msg) ->
        P = string:chr(Msg, $]),
        if P == 0 ->
                Msg;
        true ->
                string:substr(Msg, P + 2)
        end.

event(S, "WARN", Msg) ->
        disco_server:event(S#state.node, S#state.jobname,
                "~s [~s:~B] ~s", ["WARN", S#state.mode, S#state.partid, Msg],
                        [task_failed, S#state.mode]);

event(S, Type, Msg) ->
        disco_server:event(S#state.node, S#state.jobname,
                "~s [~s:~B] ~s", [Type, S#state.mode, S#state.partid, Msg], []).

parse_result(L) ->
        [PartID|Url] = string:tokens(L, " "),
        {ok, {list_to_integer(PartID), list_to_binary(Url)}}.

handle_info({_, {data, {eol, [$*,$*,$<,$M,$S,$G,$>|Line]}}}, S) ->
        event(S, "", strip_timestamp(Line)),
        {noreply, S#state{linecount = S#state.linecount + 1}};

handle_info({_, {data, {eol, [$*,$*,$<,$E,$R,$R,$>|Line]}}}, S) ->
        M = strip_timestamp(Line),
        event(S, "ERROR", M),
        gen_server:cast(disco_server, {exit_worker, self(), {job_error, M}}),
        {stop, normal, S};

handle_info({_, {data, {eol, [$*,$*,$<,$D,$A,$T,$>|Line]}}}, S) ->
        M = strip_timestamp(Line),
        event(S, "WARN", M ++ [10] ++ S#state.errlines),
        gen_server:cast(disco_server, {exit_worker, self(), {data_error, M}}),
        {stop, normal, S};

handle_info({_, {data, {eol, [$*,$*,$<,$O,$U,$T,$>|Line]}}}, S) ->
        M = strip_timestamp(Line),
        case catch parse_result(M) of
                {ok, Item} -> {noreply, S#state{results = 
                                       [Item|S#state.results]}};
                _Error -> Err = "Could not parse result line: " ++ Line,
                          event(S, "ERROR", Err),
                          gen_server:cast(disco_server, 
                                {exit_worker, self(), {job_error, Err}}),
                          {stop, normal, S}
        end;

handle_info({_, {data, {eol, [$*,$*,$<,$E,$N,$D,$>|Line]}}}, S) ->
        event(S, "", strip_timestamp(Line)),
        gen_server:cast(disco_server, 
                {exit_worker, self(), {job_ok, S#state.results}}),
        {stop, normal, S};

handle_info({_, {data, {eol, [$*,$*,$<|_] = Line}}}, S) ->
        event(S, "WARN", "Unknown line ID: " ++ Line),
        {noreply, S};               

handle_info({_, {data, {eol, Line}}}, S) ->
        {noreply, S#state{errlines = S#state.errlines ++ Line ++ [10]}};

handle_info({_, {data, {noeol, Line}}}, S) ->
        event(S, "WARN", "Truncated line: " ++ Line),
        {noreply, S};

handle_info({_, {exit_status, _Status}}, #state{linecount = 0} = S) ->
        M =  "Worker didn't start:\n" ++ S#state.errlines,
        event(S, "WARN", M),
        gen_server:cast(disco_server, {exit_worker, self(), {data_error, M}}),
        {stop, normal, S};

handle_info({_, {exit_status, _Status}}, S) ->
        M =  "Worker failed. Last words:\n" ++ S#state.errlines,
        event(S, "ERROR", M),
        gen_server:cast(disco_server, {exit_worker, self(), {job_error, M}}),
        {stop, normal, S};
        
handle_info({_, closed}, S) ->
        M = "Worker killed. Last words:\n" ++ S#state.errlines,
        event(S, "ERROR", M),
        gen_server:cast(disco_server, {exit_worker, self(), {job_error, M}}),
        {stop, normal, S};

handle_info(timeout, #state{linecount = 0} = S) ->
        M = "Worker didn't start in 30 seconds",
        event(S, "WARN", M),
        gen_server:cast(disco_server, {exit_worker, self(), {data_error, M}}),
        {stop, normal, S}.

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.              



