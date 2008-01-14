
-module(disco_worker).
-behaviour(gen_server).

-export([start_link/6, stop/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, 
        terminate/2, code_change/3]).

-define(CMD, "disco_worker.sh '~s' '~s' '~s' '~s'").
-define(PORT_OPT, [{line, 100000}, exit_status, use_stdio, stderr_to_stdout]).

start_link(From, JobName, PartID, Mode, Node, Input) ->
        {ok, Pid} = gen_server:start_link(disco_worker, 
                [From, JobName, PartID, Mode, Node, Input], []).

stop() ->
        gen_server:call(disco_worker, stop).

spawn_cmd(JobName, Node, PartID, Mode) ->
        lists:flatten(io_lib:fwrite(?CMD, [JobName, Node, PartID, Mode])).

init([From, JobName, PartID, Mode, Node, Input] = Args) ->
        ets:insert(active_workers, {self(), {JobName, From, Node}}),
        Port = open_port(
                {spawn, spawn_cmd(JobName, Node, PartID, Mode)}, ?PORT_OPT),
        port_command(Port, Input),
        {ok, {Port, Args}}.

handle_call(kill_worker, From, {Port, Args} = State) ->
        error_logger:info_report(["Kill worker"]),
        Port ! {self(), close},
        {reply, ok, State}.

handle_info({Port, {data, {eol, Line}}}, State) ->
        error_logger:info_report(["Line", Line]),
        {noreply, State};

handle_info({Port, {data, {noeol, Line}}}, State) ->
        error_logger:info_report(["TruncLine", Line]),
        {noreply, State};

handle_info({Port, {exit_status, Status}}, State) ->
        error_logger:info_report(["Exit:: ", Status]),
        {noreply, State};

handle_info({Port, closed}, State) ->
        error_logger:info_report(["Closed"]),
        {noreply, State};

handle_info({'EXIT', Port, PosixCode}, State) ->
        error_logger:info_report(["Exit: ", PosixCode]),
        {noreply, State}.

% callback stubs

terminate(_Reason, _State) -> {}.

handle_cast(_Cast, State) -> {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.              



