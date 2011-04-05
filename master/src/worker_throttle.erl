-module(worker_throttle).
-export([init/0, handle/1]).

init() ->
    none.

handle(State) ->
    {ok, 0, State}.
