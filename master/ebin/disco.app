{application, disco, [
        {description, "Disco"},
        {vsn, "1"},
        {modules, [disco_server,
                   job_queue,
                   disco_worker,
                   disco_config,
                   disco_main,
                   netstring,
                   json, 
                   trunc_io,
                   scgi,
                   scgi_server]},
        {registered, [scgi_server, disco_server, job_queue]},
        {applications, [kernel, stdlib]},
        {mod, {disco_main, []}}
]}.

