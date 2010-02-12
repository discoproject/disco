
-module(web).

-export([start/1]).


start(MochiConfig) ->
    mochiweb_http:start([{name, ddfs_get},
        {loop, fun(Req) -> loop(Req) end}
            | MochiConfig]).

loop(Req) -> 1.
%    op(Req:get(method), Req:get(path), Req).



    
    



