-module(lock_server).
-behaviour(gen_server).

-record(state, {waiters, procs}).

-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(PROC_TIMEOUT, 10 * 60 * 1000).

-spec start_link() -> no_return().
start_link() ->
    process_flag(trap_exit, true),
    case catch gen_server:start_link({local, lock_server}, lock_server, [], []) of
        {ok, _Server} ->
            ok;
        {'EXIT', Reason} ->
            exit(Reason)
    end,
    receive
        {'EXIT', _, Reason0} ->
            exit(Reason0)
    end.

init(_Args) ->
    process_flag(trap_exit, true),
    {ok, #state{waiters = gb_trees:empty(),
                procs = gb_trees:empty()}}.

handle_call({wait, Key, Proc}, From, S) ->
    {WaiterList1, S1} =
        case gb_trees:lookup(Key, S#state.waiters) of
            none ->
                Pid = spawn_link(Proc),
                {ok, TRef} = timer:kill_after(?PROC_TIMEOUT, Pid),
                Procs = gb_trees:insert(Pid, {Key, TRef}, S#state.procs),
                {[], S#state{procs = Procs}};
            {value, WaiterList} ->
                {WaiterList, S}
        end,
    Waiters = gb_trees:enter(Key, [From|WaiterList1], S1#state.waiters),
    {noreply, S1#state{waiters = Waiters}}.

handle_info({'EXIT', Pid, Reason}, S) ->
    {value, {Key, TRef}} = gb_trees:lookup(Pid, S#state.procs),
    {value, Waiters} = gb_trees:lookup(Key, S#state.waiters),
    Msg = if Reason =:= normal -> ok; true -> {error, Reason} end,
    _ = [gen_server:reply(From, Msg) || From <- Waiters],
    _ = timer:cancel(TRef),
    {noreply, S#state{procs = gb_trees:delete(Pid, S#state.procs),
                      waiters = gb_trees:delete(Key, S#state.waiters)}}.

handle_cast(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

