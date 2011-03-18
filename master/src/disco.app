{application, disco, [
        {description, "Disco"},
        {vsn, "%DISCO_VERSION%"},
        {modules, [disco_server,
                   job_queue,
                   event_server,
                   disco_worker,
                   disco_config,
                   disco_main,
                   netstring,
                   json,
                   trunc_io,
                   scgi,
                   scgi_server]},
        {registered, [scgi_server, event_server, disco_server, job_queue]},
        {applications, [kernel, stdlib]},
        {mod, {disco_main, []}}
]}.

